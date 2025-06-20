import scrapy
import json
import csv
import io
import re
import traceback
import logging
import time
import gc
from typing import Dict, Any, Optional, Union, List
from scrapy import signals
from scrapy.signalmanager import dispatcher
from scrapy.http import Request
from scrapy.crawler import CrawlerRunner, CrawlerProcess
from scrapy.utils.project import get_project_settings
from twisted.internet import defer, reactor, task
from dataclasses import dataclass
import boto3
from scrapy.utils.log import configure_logging
from multiprocessing import Queue
import os

# Configure logging for multiprocessing
def setup_logging(process_name):
    configure_logging({
        'LOG_LEVEL': 'INFO',
        'LOG_FORMAT': f'[{process_name}] %(levelname)s: %(message)s',
        'LOG_STDOUT': True,
        'LOG_FILE': None
    })
    logger = logging.getLogger(f"DealerSpider_{process_name}")
    
    # Disable scrapy's default logging
    logging.getLogger('scrapy.core.scraper').setLevel(logging.WARNING)
    logging.getLogger('scrapy.core.engine').setLevel(logging.WARNING)
    logging.getLogger('scrapy.downloadermiddlewares.redirect').setLevel(logging.WARNING)
    logging.getLogger('scrapy.downloadermiddlewares.retry').setLevel(logging.WARNING)
    
    return logger

@dataclass
class ScrapingPattern:
    selector: list[str]
    attr: Optional[str] = None
    multiple: bool = False
    count: bool = False
    custom: bool = False
    filter: Optional[str] = None
    transform: Optional[str] = None
    regex: Optional[str] = None

class DealerSpider(scrapy.Spider):
    name = "dealer_spider"
    
    def __init__(self, urls=None, css_patterns=None, s3_bucket=None, s3_key_prefix=None, 
                 receipt_handle=None, queue_url=None, dealer_name=None, process_name=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.urls = urls or []
        self.patterns = self._process_patterns(css_patterns) if css_patterns else {}
        self.s3_bucket = s3_bucket
        self.s3_key_prefix = s3_key_prefix
        self.receipt_handle = receipt_handle
        self.queue_url = queue_url
        self.dealer_name = dealer_name or 'unknown_dealer'
        self.process_name = process_name or 'unknown'
        self.scraped_items = []
        
        # Use a separate logger instance instead of trying to overwrite the spider's logger
        self.custom_logger = setup_logging(f"{self.process_name}_{self.dealer_name}")
        
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.custom_logger.info(f"Initialized spider for dealer '{self.dealer_name}' with {len(self.urls)} URLs")

    def _process_patterns(self, css_patterns):
        processed = {}
        if not css_patterns:
            return processed

        for k, v in css_patterns.items():
            try:
                if isinstance(v, ScrapingPattern):
                    processed[k] = v
                    continue

                if isinstance(v, dict):
                    original_attr = v.get('attr')
                    
                    if 'selector' in v:
                        if isinstance(v['selector'], str):
                            selector, attr = self._process_selector(v['selector'])
                            v['selector'] = [selector]
                            if original_attr is None and attr:
                                v['attr'] = attr
                            elif original_attr is not None:
                                v['attr'] = original_attr
                        elif isinstance(v['selector'], list):
                            processed_selectors = []
                            detected_attr = None
                            
                            for sel in v['selector']:
                                selector, attr = self._process_selector(str(sel))
                                processed_selectors.append(selector)
                                if detected_attr is None and attr:
                                    detected_attr = attr
                            
                            v['selector'] = processed_selectors
                            if original_attr is None and detected_attr:
                                v['attr'] = detected_attr
                            elif original_attr is not None:
                                v['attr'] = original_attr

                    filtered_dict = {
                        key: v[key] for key in v 
                        if key in {'selector', 'attr', 'multiple', 'count', 'custom', 'filter', 'transform', 'regex'}
                    }
                    processed[k] = ScrapingPattern(**filtered_dict)
                else:
                    selector, attr = self._process_selector(str(v))
                    processed[k] = ScrapingPattern(
                        selector=[selector],
                        attr=attr
                    )
            except Exception as e:
                if hasattr(self, 'logger'):
                    self.logger.warning(f"Invalid pattern for key '{k}', using default: {str(e)}")
                processed[k] = ScrapingPattern(selector=[""])

        return processed

    def _process_selector(self, selector: str) -> tuple[str, Optional[str]]:
        attr_match = re.search(r':+\s*attr\((.*?)\)$', selector)
        if attr_match:
            base_selector = selector.replace(attr_match.group(0), '')
            return base_selector, attr_match.group(1)

        text_match = re.search(r':+\s*text\s*$', selector)
        if text_match:
            base_selector = selector.replace(text_match.group(0), '')
            return base_selector, 'text'

        return selector, None

    def start_requests(self):
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)

    def _apply_transform(self, value: str, transform: str) -> str:
        if transform == 'deduplicate_stock':
            parts = value.split()
            seen = set()
            result = []
            for part in parts:
                if part not in seen:
                    seen.add(part)
                    result.append(part)
            return ' '.join(result)
        return value

    def parse(self, response):
        sel = scrapy.Selector(response)
        item = {'source_url': response.url}
        self.logger.info(f"Processing URL: {response.url}")

        for field, pattern in self.patterns.items():
            try:
                if pattern.count:
                    item[field] = len(sel.css(pattern.selector[0]))
                    continue

                results = []
                for selector in pattern.selector:
                    elements = sel.css(selector)
                    if not elements:
                        continue

                    if pattern.multiple:
                        for element in elements:
                            if pattern.attr == 'text':
                                value = self._clean_text(element.css('::text').get())
                            elif pattern.attr:
                                value = self._clean_text(element.css(f'::attr({pattern.attr})').get())
                            else:
                                value = self._clean_text(element.css('::text').get())

                            if value and pattern.regex:
                                value = self._apply_regex(value, pattern.regex)
                            
                            if value and pattern.transform:
                                value = self._apply_transform(value, pattern.transform)

                            if value:
                                results.append(value)
                    else:
                        if field in {"price", "msrp"}:
                            if pattern.attr == 'text':
                                value = self._clean_text(elements.css('::text').get(default='').strip())
                            elif pattern.attr:
                                value = self._clean_text(elements.css(f'::attr({pattern.attr})').get())
                            else:
                                value = self._clean_text(elements.css('::text').get(default='').strip())
                            
                            if pattern.regex:
                                value = self._apply_regex(value, pattern.regex)
                            
                            value = self._clean_price_text(value)
                            
                            if not value:
                                all_text = self._clean_text(elements.css('*::text').getall())
                                if pattern.regex:
                                    all_text = self._apply_regex(all_text, pattern.regex)
                                value = self._clean_price_text(all_text)
                            
                            if value:
                                results.append(value)
                                break
                        else:
                            if pattern.attr == 'text':
                                value = self._clean_text(elements.css('*::text').getall())
                            elif pattern.attr:
                                value = self._clean_text(elements.css(f'::attr({pattern.attr})').get())
                            else:
                                value = self._clean_text(elements.css('*::text').getall())

                            if value and pattern.regex:
                                value = self._apply_regex(value, pattern.regex)

                            if value and pattern.transform:
                                value = self._apply_transform(value, pattern.transform)

                            if value:
                                results.append(value)
                                break

                if pattern.multiple:
                    item[field] = '|'.join(results) if results else None
                else:
                    item[field] = results[0] if results else None

            except Exception as e:
                self.logger.error(f"Error extracting {field}: {e}")
                item[field] = None if pattern.multiple else ''

        self.scraped_items.append(item)
        return item

    def _clean_text(self, text):
        if not text:
            return ""
        if isinstance(text, list):
            text = " ".join(text)
        return text.strip()

    def _clean_price_text(self, text):
        if not text:
            return ""
        if isinstance(text, list):
            text = " ".join(text)
        
        text = text.replace('$', '').replace(',', '')
        if '.' in text:
            text = text.split('.')[0]
        return text.strip()

    def _apply_regex(self, text: str, pattern: str) -> str:
        try:
            match = re.search(pattern, text)
            if match:
                if match.lastindex:
                    return match.group(1)
                else:
                    return match.group(0)
            return ""
        except Exception as e:
            self.logger.warning(f"Regex failed on text '{text}' with pattern '{pattern}': {e}")
            return ""

    def spider_closed(self, spider):
        if spider is self:
            try:
                self.logger.info(f"Spider closing, processed {len(self.scraped_items)} items")
                if self.s3_bucket and self.s3_key_prefix:
                    # self._upload_to_s3()
                    pass
            except Exception as e:
                self.logger.error(f"Error in spider_closed: {e}")

    def _upload_to_s3(self):
        """Upload scraped data to S3 (currently commented as requested)"""
        MAX_RETRIES = 3
        attempt = 0
        csv_buffer = None
        
        while attempt < MAX_RETRIES:
            try:
                self.logger.info(f"Starting S3 upload attempt {attempt + 1}/{MAX_RETRIES} with {len(self.scraped_items)} items")
                
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=['source_url'] + list(self.patterns.keys()))
                writer.writeheader()
                writer.writerows(self.scraped_items)
                
                s3_client = boto3.client('s3', region_name='us-east-1')
                s3_key = f"{self.s3_key_prefix}.csv"
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=csv_buffer.getvalue().encode('utf-8'),
                    ContentType='text/csv'
                )            
                
                s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
                
                self.logger.info(f"Successfully uploaded to s3://{self.s3_bucket}/{s3_key}")
                
                if self.receipt_handle and self.queue_url:
                    sqs_client = boto3.client('sqs', region_name='us-east-1')
                    sqs_client.delete_message(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=self.receipt_handle
                    )
                    self.logger.info("Deleted SQS message after successful upload")
                
                self.scraped_items = []
                return True
                
            except Exception as e:
                attempt += 1
                self.logger.error(f"S3 upload attempt {attempt} failed: {e}")
                if attempt >= MAX_RETRIES:
                    self.logger.error(f"Failed to upload after {MAX_RETRIES} attempts")
                    raise
            finally:
                if csv_buffer:
                    csv_buffer.close()

# Scrapy settings optimized for multiprocessing
def get_spider_settings():
    settings = get_project_settings()
    settings.update({
        'CONCURRENT_SPIDERS': 1,                    # One spider per process
        'CONCURRENT_REQUESTS': 30,                  # 30 concurrent requests per spider
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,        # Max 2 requests per domain
        'REACTOR_THREADPOOL_MAXSIZE': 30,
        'DOWNLOAD_DELAY': 0.5,
        
        # User Agent settings
        'USER_AGENT_LIST': [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59',
        ],
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            __name__ + '.RotateUserAgentMiddleware': 400,
        },
        
        # AutoThrottle settings
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 10,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 2.0,
        
        # Retry strategy
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 2,
        'RETRY_HTTP_CODES': [429, 500, 502, 503, 504],
        
        # Disable telnet console to avoid port conflicts
        'TELNETCONSOLE_ENABLED': False,
        
        # Memory optimization
        'MEMDEBUG_ENABLED': False,
        'MEMUSAGE_ENABLED': True,
        'MEMUSAGE_WARNING_MB': 512,
        'MEMUSAGE_LIMIT_MB': 1024,
    })
    return settings

# User Agent Middleware
import random
class RotateUserAgentMiddleware:
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59',
        ]
    
    def process_request(self, request, spider):
        user_agent = random.choice(self.user_agents)
        request.headers['User-Agent'] = user_agent
        return None

def run_single_spider(job_data, process_name):
    """Run a single spider in a separate process"""
    try:
        logger = setup_logging(f"{process_name}_{job_data.get('dealer_name', 'unknown')}")
        logger.info(f"Starting spider for dealer: {job_data.get('dealer_name', 'unknown')}")
        
        # Create and configure process
        process = CrawlerProcess(get_spider_settings())
        
        # Add spider to process
        process.crawl(
            DealerSpider,
            urls=job_data['urls'],
            css_patterns=job_data['css_patterns'],
            s3_bucket=job_data['s3_bucket'],
            s3_key_prefix=job_data['s3_key_prefix'],
            receipt_handle=job_data.get('receipt_handle'),
            queue_url=job_data.get('queue_url'),
            dealer_name=job_data.get('dealer_name', 'unknown'),
            process_name=process_name
        )
        
        # Start the reactor
        process.start()
        
        logger.info(f"Spider completed for dealer: {job_data.get('dealer_name', 'unknown')}")
        return {'success': True, 'dealer_name': job_data.get('dealer_name', 'unknown')}
        
    except Exception as e:
        error_msg = f"Spider failed for dealer {job_data.get('dealer_name', 'unknown')}: {e}"
        print(error_msg)
        traceback.print_exc()
        return {'success': False, 'dealer_name': job_data.get('dealer_name', 'unknown'), 'error': str(e)}

def run_spider_process(job_queue: Queue, result_queue: Queue, shutdown_event):
    """Worker process that runs spiders from job queue"""
    process_name = f"Process-{os.getpid()}"
    logger = setup_logging(process_name)
    logger.info(f"Spider worker process {process_name} started")
    
    while not shutdown_event.is_set():
        try:
            # Get job from queue with timeout
            try:
                job_data = job_queue.get(timeout=5)
            except:
                # No job available, check if we should continue
                if job_queue.empty():
                    logger.info(f"No more jobs available, worker {process_name} exiting")
                    break
                continue
            
            if job_data is None:  # Poison pill to stop worker
                logger.info(f"Received stop signal, worker {process_name} exiting")
                break
            
            logger.info(f"Processing job for dealer: {job_data.get('dealer_name', 'unknown')}")
            
            # Run the spider
            result = run_single_spider(job_data, process_name)
            
            # Put result in result queue
            result_queue.put(result)
            
            # Mark job as done
            job_queue.task_done()
            
            # Small delay between jobs
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error in worker process {process_name}: {e}")
            traceback.print_exc()
            
            # Put error result
            result_queue.put({
                'success': False, 
                'dealer_name': job_data.get('dealer_name', 'unknown') if 'job_data' in locals() else 'unknown',
                'error': str(e)
            })
            
            if 'job_data' in locals():
                job_queue.task_done()
    
    logger.info(f"Spider worker process {process_name} finished")

def run_batch_spiders_multiprocess(dealer_jobs, max_processes=3):
    """
    Run multiple spider jobs using multiprocessing
    Each process runs one spider at a time with 30 concurrent requests and max 2 per domain
    """
    if not dealer_jobs:
        print("No dealer jobs to process")
        return True
    
    print(f"Starting batch processing with {len(dealer_jobs)} jobs using {max_processes} processes")
    
    # Create job and result queues
    job_queue = Queue()
    result_queue = Queue()
    shutdown_event = mp.Event()
    
    # Add all jobs to the queue
    for job in dealer_jobs:
        job_queue.put(job)
    
    # Create and start worker processes
    processes = []
    num_processes = min(max_processes, len(dealer_jobs))
    
    for i in range(num_processes):
        process = mp.Process(
            target=run_spider_process,
            args=(job_queue, result_queue, shutdown_event),
            name=f"SpiderWorker-{i+1}"
        )
        process.start()
        processes.append(process)
    
    print(f"Started {num_processes} spider worker processes")
    
    # Collect results
    completed_jobs = 0
    total_jobs = len(dealer_jobs)
    success_count = 0
    results = []
    
    # Set timeout for waiting for results (5 minutes per job)
    total_timeout = max(300, total_jobs * 60)  # At least 5 minutes, or 1 minute per job
    start_time = time.time()
    
    while completed_jobs < total_jobs:
        try:
            # Check for timeout
            if time.time() - start_time > total_timeout:
                print(f"Timeout reached ({total_timeout}s), stopping batch processing")
                shutdown_event.set()
                break
            
            # Get result with timeout
            result = result_queue.get(timeout=30)
            completed_jobs += 1
            results.append(result)
            
            if result.get('success', False):
                success_count += 1
                print(f"✓ Job {completed_jobs}/{total_jobs} completed: {result.get('dealer_name', 'unknown')}")
            else:
                error_msg = result.get('error', 'Unknown error')
                print(f"✗ Job {completed_jobs}/{total_jobs} failed: {result.get('dealer_name', 'unknown')} - {error_msg}")
                
        except Exception as e:
            print(f"Error waiting for results: {e}")
            # Check if processes are still alive
            alive_processes = [p for p in processes if p.is_alive()]
            if not alive_processes:
                print("All worker processes have died, stopping batch")
                break
    
    # Signal shutdown and clean up processes
    shutdown_event.set()
    
    print("Waiting for worker processes to complete...")
    for process in processes:
        process.join(timeout=30)
        if process.is_alive():
            print(f"Force terminating process {process.name}")
            process.terminate()
            process.join(timeout=5)
    
    # Clear queues
    while not job_queue.empty():
        try:
            job_queue.get_nowait()
        except:
            break
    
    while not result_queue.empty():
        try:
            result_queue.get_nowait()
        except:
            break
    
    print(f"Batch processing completed: {success_count}/{total_jobs} jobs successful")
    return success_count == total_jobs, results

# Import multiprocessing at module level
import multiprocessing as mp