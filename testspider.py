import scrapy
import json
import csv
import io
import re
import traceback
import logging
from typing import Dict, Any, Optional, Union, List
from scrapy import signals
from scrapy.signalmanager import dispatcher
from scrapy.http import Request
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import defer, reactor
from dataclasses import dataclass
import boto3
from scrapy.utils.log import configure_logging

# Configure logging
configure_logging({
    'LOG_LEVEL': 'INFO',  # Change to INFO level
    'LOG_FORMAT': '%(levelname)s: %(message)s',
    'LOG_STDOUT': False,
    'LOG_FILE': 'spider.log'
})
logger = logging.getLogger("DealerSpider")

# Disable scrapy's default logging
logging.getLogger('scrapy.core.scraper').setLevel(logging.WARNING)
logging.getLogger('scrapy.core.engine').setLevel(logging.WARNING)

@dataclass
class ScrapingPattern:
    selector: list[str]
    attr: Optional[str] = None
    multiple: bool = False
    count: bool = False
    custom: bool = False
    filter: Optional[str] = None
    transform: Optional[str] = None
    regex: Optional[str] = None  # <-- Added regex support

class DealerSpider(scrapy.Spider):
    name = "dealer_spider"
    spider_items = {}

    def __init__(self, urls=None, css_patterns=None, s3_bucket=None, s3_key_prefix=None, receipt_handle=None, queue_url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spider_id = id(self)
        self.urls = urls or []
        self.patterns = self._process_patterns(css_patterns) if css_patterns else {}
        self.s3_bucket = s3_bucket
        self.s3_key_prefix = s3_key_prefix
        self.receipt_handle = receipt_handle
        self.queue_url = queue_url
        self.spider_items[self.spider_id] = []
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        logger.info(f"Initialized spider {self.spider_id} for dealer with {len(urls)} URLs")

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
                    
                    # Handle both string and list selectors
                    if 'selector' in v:
                        if isinstance(v['selector'], str):
                            # Single string selector
                            selector, attr = self._process_selector(v['selector'])
                            v['selector'] = [selector]
                            if original_attr is None and attr:
                                v['attr'] = attr
                            elif original_attr is not None:
                                v['attr'] = original_attr
                        elif isinstance(v['selector'], list):
                            # List of selectors - process each one
                            processed_selectors = []
                            detected_attr = None
                            
                            for sel in v['selector']:
                                selector, attr = self._process_selector(str(sel))
                                processed_selectors.append(selector)
                                # Use the first detected attr if no original_attr specified
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
                    # Handle simple string patterns
                    selector, attr = self._process_selector(str(v))
                    processed[k] = ScrapingPattern(
                        selector=[selector],
                        attr=attr
                    )
            except Exception as e:
                logger.warning(f"Invalid pattern for key '{k}', using default: {str(e)}")
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
            # Split the string by spaces and remove duplicate sequences
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
        logger.info(f"Spider {self.spider_id} processing URL: {response.url}")

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
                            # FIXED: Handle price/msrp fields correctly for list selectors
                            if pattern.attr == 'text':
                                value = self._clean_text(elements.css('::text').get(default='').strip())
                            elif pattern.attr:
                                value = self._clean_text(elements.css(f'::attr({pattern.attr})').get())
                            else:
                                value = self._clean_text(elements.css('::text').get(default='').strip())
                            
                            # Apply regex if exists
                            if pattern.regex:
                                value = self._apply_regex(value, pattern.regex)
                            
                            # Clean the price
                            value = self._clean_price_text(value)
                            
                            # If we got empty value, try getting all text content as fallback
                            if not value:
                                all_text = self._clean_text(elements.css('*::text').getall())
                                if pattern.regex:
                                    all_text = self._apply_regex(all_text, pattern.regex)
                                value = self._clean_price_text(all_text)
                            
                            # FIXED: Add the value to results and continue to next selector if empty
                            if value:
                                results.append(value)
                                break  # Found a value, stop trying other selectors
                            # If no value found, continue to next selector in the list
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
                logger.error(f"Error extracting {field}: {e}")
                item[field] = None if pattern.multiple else ''

        self.spider_items[self.spider_id].append(item)
        logger.info(f"Spider {self.spider_id} processed URL: {response.url}")
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
        
        # Remove $ and commas
        text = text.replace('$', '').replace(',', '')
        # Remove decimal point and everything after it
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
            return ""  # Return empty string if no match found
        except Exception as e:
            logger.warning(f"Regex failed on text '{text}' with pattern '{pattern}': {e}")
            return ""  # Also return empty string on exception

    def spider_closed(self, spider):
        if spider is self:  # Only process if this is the signal for this specific spider
            try:
                logger.info(f"Spider {self.spider_id} closing, processing {len(self.spider_items[self.spider_id])} items")
                if self.s3_bucket and self.s3_key_prefix:
                    self._upload_to_s3()
            finally:
                # Clean up this spider's data
                if self.spider_id in self.spider_items:
                    del self.spider_items[self.spider_id]
                    logger.info(f"Cleaned up data for spider {self.spider_id}")

    def _upload_to_s3(self):
        MAX_RETRIES = 3
        attempt = 0
        csv_buffer = None
        
        while attempt < MAX_RETRIES:
            try:
                items = self.spider_items[self.spider_id]
                logger.info(f"Spider {self.spider_id} starting S3 upload attempt {attempt + 1}/{MAX_RETRIES} with {len(items)} items")
                
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=['source_url'] + list(self.patterns.keys()))
                writer.writeheader()
                writer.writerows(items)
                
                s3_client = boto3.client('s3',region_name='us-east-1')
                s3_key = f"{self.s3_key_prefix}.csv"
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=csv_buffer.getvalue().encode('utf-8'),
                    ContentType='text/csv'
                )            
                
                # Verify the upload by trying to get the object
                s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
                
                logger.info(f"Spider {self.spider_id} successfully uploaded to s3://{self.s3_bucket}/{s3_key}")
                
                # Delete SQS message after successful upload
                if self.receipt_handle and self.queue_url:
                    sqs_client = boto3.client('sqs',region_name='us-east-1')
                    sqs_client.delete_message(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=self.receipt_handle
                    )
                    logger.info(f"Spider {self.spider_id} deleted SQS message after successful upload")
                
                # Clear items only after successful upload
                self.spider_items[self.spider_id] = []
                return True
                
            except Exception as e:
                attempt += 1
                logger.error(f"Spider {self.spider_id} S3 upload attempt {attempt} failed: {e}")
                if attempt >= MAX_RETRIES:
                    logger.error(f"Spider {self.spider_id} failed to upload after {MAX_RETRIES} attempts")
                    raise
            finally:
                if csv_buffer:
                    csv_buffer.close()
                    del csv_buffer

# ---------------------- SCALABLE CRAWL RUNNER PART ----------------------

settings = get_project_settings()
settings.update({
    'CONCURRENT_SPIDERS': 15,                   # 10 spiders in parallel
    'CONCURRENT_REQUESTS': 30,                 # 10 spiders × 2 requests each
    'CONCURRENT_REQUESTS_PER_DOMAIN': 2,       # Keep it low per domain
    'REACTOR_THREADPOOL_MAXSIZE': 30,          # Adequate for current concurrency
    'DOWNLOAD_DELAY': 0.5,                    # Small delay between requests

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
    'AUTOTHROTTLE_TARGET_CONCURRENCY': 2.0,    # Each spider targets 4 req/sec

    # Retry strategy
    'RETRY_ENABLED': True,
    'RETRY_TIMES': 2,
    'RETRY_HTTP_CODES': [429, 500, 502, 503, 504]
})



runner = CrawlerRunner(settings)

def schedule_spider(urls, css_patterns, s3_bucket=None, s3_key_prefix=None):
    """Schedule a spider crawl without running reactor immediately"""
    return runner.crawl(
        DealerSpider,
        urls=urls,
        css_patterns=css_patterns,
        s3_bucket=s3_bucket,
        s3_key_prefix=s3_key_prefix
    )

@defer.inlineCallbacks
def crawl_all_jobs(jobs):
    """Crawl batch of jobs (dealers) concurrently"""
    deferreds = []
    try:
        # Create deferred list for parallel execution
        for job in jobs:
            d = runner.crawl(
                DealerSpider,
                urls=job['urls'],
                css_patterns=job['css_patterns'],
                s3_bucket=job['s3_bucket'],
                s3_key_prefix=job['s3_key_prefix'],
                receipt_handle=job.get('receipt_handle'),
                queue_url=job.get('queue_url')
            )
            deferreds.append(d)
        
        # Wait for all spiders to complete and get results
        results = yield defer.DeferredList(deferreds, consumeErrors=True)
        
        # Process results with detailed logging
        successful_spiders = 0
        failed_spiders = 0
        
        for i, (success, result) in enumerate(results):
            if success:
                successful_spiders += 1
                logger.info(f"Spider {i + 1}/{len(results)} completed successfully")
            else:
                failed_spiders += 1
                logger.error(f"Spider {i + 1}/{len(results)} failed: {result.value if hasattr(result, 'value') else 'Unknown error'}")
        
        success = successful_spiders == len(results)  # Only true if ALL spiders succeeded
        logger.info(f"Batch summary: {successful_spiders} succeeded, {failed_spiders} failed")
        defer.returnValue(success)
        
    except Exception as e:
        logger.error(f"Error in batch crawl: {e}")
        defer.returnValue(False)
    finally:
        deferreds.clear()
        del deferreds
        logger.info("Cleared deferreds list")

# Add User Agent Middleware
import random
class RotateUserAgentMiddleware:
    def process_request(self, request, spider):
        user_agent = random.choice(settings.get('USER_AGENT_LIST'))
        request.headers['User-Agent'] = user_agent
        return None