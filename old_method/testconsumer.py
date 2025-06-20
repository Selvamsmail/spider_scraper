import boto3
import json
import traceback
import sys
import gc
from datetime import datetime
from sitemap_extractor import SitemapExtractor
from testspider import crawl_all_jobs
from twisted.internet import reactor, defer, task
from twisted.internet.threads import deferToThread

# AWS clients
sqs = boto3.client('sqs', region_name='us-east-1')
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/884203033942/webscraping'
S3_BUCKET = 'rapidious-datalake'


# sqs = boto3.client(
#     'sqs',
#     region_name='us-east-1',
#     aws_access_key_id='test',
#     aws_secret_access_key='test',
#     endpoint_url='http://localhost:4566'
# )
# QUEUE_URL = 'http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/webscraping'


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
            print(f"No URLs found for dealer: {message_body.get('dealer_name', 'unknown')}")
            # Delete message immediately if no URLs found
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            print(f"Deleted message for dealer with no URLs: {message_body.get('dealer_name', 'unknown')}")
            return None

        url_list = [u['dp_url'] for u in urls]
        urls = None 

        css_patterns = message_body.get('css_pattern', {})
        dealer_name = message_body.get('dealer_name', 'unknown_dealer')
        s3_key_prefix = f"webscraping/tester_output/{current_date}/{dealer_name}"
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
        print(f"Error processing message: {e}")
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
                    print(f"Error fetching from SQS: {e}")
                    break

                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed >= MAX_WAIT_SECONDS:
                    print("Max wait time reached while polling SQS.")
                    break

            if not collected_messages:
                print("No more messages in SQS queue. Stopping reactor...")
                reactor.stop()
                return

            print(f"Collected {len(collected_messages)} messages from SQS.")

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
                    print(f"Message {i + 1} failed to process.")
                    if not success and hasattr(result, 'printTraceback'):
                        result.printTraceback()
                result = None  # Clear result object

            # Clear processed messages
            deferred_jobs = None
            message_map = None
            results = None

            # Step 4: Run spiders in parallel
            if dealer_jobs:
                print(f"Starting crawl for {len(dealer_jobs)} dealers...")

                try:
                    # Wait for crawl completion and check success
                    crawl_success = yield crawl_all_jobs(dealer_jobs)
                    if not crawl_success:
                        print("Some spiders failed to complete successfully")
                    else:
                        print(f"Batch #{batch_count + 1} completed successfully")
                except Exception as e:
                    print(f"Error during crawl_all_jobs: {e}")
                    traceback.print_exc()
            else:
                print("No valid jobs in this batch, moving to next.")

            batch_count += 1

        finally:
            # Clean up batch variables
            dealer_jobs = None
            receipt_handles = None
            collected_messages = None
            response = None
            gc.collect()  # Force garbage collection
            gc.collect()  # Second pass to clean up any circular references


if __name__ == "__main__":
    try:
        task.deferLater(reactor, 0, poll_sqs_and_scrape)
        reactor.run()
        print("Finished scraping all dealers.")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal script error: {e}")
        traceback.print_exc()
        sys.exit(1)