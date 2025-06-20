import boto3
import json
import traceback
import sys
import gc
import multiprocessing as mp
import os
from datetime import datetime
from sitemap_extractor import SitemapExtractor
from testspider import crawl_all_jobs
from twisted.internet import reactor, defer, task
from twisted.internet.threads import deferToThread

# AWS clients
sqs = boto3.client('sqs', region_name='us-east-1')
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/884203033942/webscraping'
S3_BUCKET = 'rapidious-datalake'


def process_message(message_body, receipt_handle, queue_url):
    try:
        current_date = datetime.now().strftime("%Y%m%d")
        processed_data = None

        if isinstance(message_body, str):
            message_body = json.loads(message_body)

        if 'css_pattern' in message_body:
            pattern_str = message_body['css_pattern']
            # pattern_str = pattern_str.replace('""', '"').strip('"\'')
            message_body['css_pattern'] = json.loads(pattern_str)
            pattern_str = None  # Clear string after use

        for key in ['sitemap_urls', 'patterns']:
            if key in message_body:
                value = message_body[key]
                if isinstance(value, str):
                    try:
                        message_body[key] = json.loads(value.strip('"').replace('\\"', '"'))
                    except json.JSONDecodeError:
                        message_body[key] = [] if key in ['sitemap_urls', 'patterns'] else {}
                    value = None  # Clear string after use

        extractor = SitemapExtractor()
        urls = extractor.get_filtered_urls_from_source(message_body)
        if not urls:
            print(f"Process {os.getpid()}: No URLs found for dealer: {message_body.get('dealer_name', 'unknown')}")
            # Delete message immediately if no URLs found
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            print(f"Process {os.getpid()}: Deleted message for dealer with no URLs: {message_body.get('dealer_name', 'unknown')}")
            return None

        url_list = [u['dp_url'] for u in urls]
        urls = None 

        css_patterns = message_body.get('css_pattern', {})
        dealer_name = message_body.get('dealer_name', 'unknown_dealer')
        s3_key_prefix = f"webscraping/testing_output/{current_date}/{dealer_name}"
        # s3_key_prefix = f"webscraping/dealer_output/20250608/{dealer_name}"

        processed_data = {
            'urls': url_list,
            'css_patterns': css_patterns,
            's3_bucket': S3_BUCKET,
            's3_key_prefix': s3_key_prefix,
            'receipt_handle': receipt_handle,
            'queue_url': queue_url
        }

        # Clear large objects
        message_body = None
        url_list = None
        css_patterns = None
        
        return processed_data

    except Exception as e:
        print(f"Process {os.getpid()}: Error processing message: {e}")
        traceback.print_exc()
        return None
    finally:
        gc.collect()  # Force garbage collection

@defer.inlineCallbacks
def poll_sqs_and_scrape():
    batch_count = 0
    MAX_BATCH_SIZE = 15
    MAX_WAIT_SECONDS = 30

    while True:
        dealer_jobs = []
        receipt_handles = []
        deferred_jobs = []
        message_map = {}
        collected_messages = []
        response = None

        try:
            start_time = datetime.now()

            while len(collected_messages) < MAX_BATCH_SIZE:
                try:
                    response = sqs.receive_message(
                        QueueUrl=QUEUE_URL,
                        MaxNumberOfMessages=1,
                        WaitTimeSeconds=10
                    )
                    messages = response.get('Messages', [])
                    if messages:
                        collected_messages.extend(messages)
                    response = None  # Clear response object

                except Exception as e:
                    print(f"Process {os.getpid()}: Error fetching from SQS: {e}")
                    break

                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed >= MAX_WAIT_SECONDS:
                    print(f"Process {os.getpid()}: Max wait time reached while polling SQS.")
                    break

            if not collected_messages:
                print(f"Process {os.getpid()}: No more messages in SQS queue. Stopping reactor...")
                reactor.stop()
                return

            print(f"Process {os.getpid()}: Collected {len(collected_messages)} messages from SQS.")

            # Step 2: Process messages in parallel threads
            for message in collected_messages:
                message_body = message['Body']
                d = deferToThread(process_message, message_body, message['ReceiptHandle'], QUEUE_URL)
                deferred_jobs.append(d)
                message_body = None  # Clear message body

            # Clear collected messages as they're no longer needed
            collected_messages = None

            # Step 3: Wait for all processing to complete
            results = yield defer.DeferredList(deferred_jobs, consumeErrors=True)

            for i, (success, result) in enumerate(results):
                if success and result:
                    dealer_jobs.append(result)
                else:
                    print(f"Process {os.getpid()}: Message {i + 1} failed to process.")
                    if not success and hasattr(result, 'printTraceback'):
                        result.printTraceback()
                result = None  # Clear result object

            # Clear processed messages
            deferred_jobs = None
            message_map = None
            results = None

            # Step 4: Run spiders in parallel
            if dealer_jobs:
                print(f"Process {os.getpid()}: Starting crawl for {len(dealer_jobs)} dealers...")

                try:
                    # Wait for crawl completion and check success
                    crawl_success = yield crawl_all_jobs(dealer_jobs)
                    if not crawl_success:
                        print(f"Process {os.getpid()}: Some spiders failed to complete successfully")
                    else:
                        print(f"Process {os.getpid()}: Batch #{batch_count + 1} completed successfully")
                except Exception as e:
                    print(f"Process {os.getpid()}: Error during crawl_all_jobs: {e}")
                    traceback.print_exc()
            else:
                print(f"Process {os.getpid()}: No valid jobs in this batch, moving to next.")

            batch_count += 1

        finally:
            # Clean up batch variables
            dealer_jobs = None
            receipt_handles = None
            collected_messages = None
            response = None
            gc.collect()  # Force garbage collection
            gc.collect()  # Second pass to clean up any circular references


def worker_process(process_id):
    """Worker process function that runs the scraping loop"""
    print(f"Starting worker process {process_id} with PID {os.getpid()}")
    try:
        task.deferLater(reactor, 0, poll_sqs_and_scrape)
        reactor.run()
        print(f"Process {process_id} (PID {os.getpid()}): Finished scraping all dealers.")
    except Exception as e:
        print(f"Process {process_id} (PID {os.getpid()}): Fatal script error: {e}")
        traceback.print_exc()
    finally:
        print(f"Process {process_id} (PID {os.getpid()}): Worker process exiting.")


if __name__ == "__main__":
    try:
        # Get CPU count and create that many processes
        cpu_count = mp.cpu_count()
        print(f"System has {cpu_count} CPU cores. Starting {cpu_count} worker processes...")
        
        # Create and start worker processes
        processes = []
        for i in range(cpu_count):
            p = mp.Process(target=worker_process, args=(i,))
            p.start()
            processes.append(p)
            print(f"Started worker process {i} with PID {p.pid}")
        
        # Wait for all processes to complete
        for i, p in enumerate(processes):
            p.join()
            print(f"Worker process {i} (PID {p.pid}) has finished.")
        
        print("All worker processes have completed.")
        sys.exit(0)
        
    except KeyboardInterrupt:
        print("Received interrupt signal. Terminating all processes...")
        for p in processes:
            if p.is_alive():
                p.terminate()
        
        # Wait for processes to terminate
        for i, p in enumerate(processes):
            p.join(timeout=5)
            if p.is_alive():
                print(f"Force killing process {i} (PID {p.pid})")
                p.kill()
        
        sys.exit(1)
    except Exception as e:
        print(f"Fatal script error: {e}")
        traceback.print_exc()
        sys.exit(1)