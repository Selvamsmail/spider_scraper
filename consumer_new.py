import boto3
import json
import traceback
import sys
import gc
import multiprocessing as mp
from multiprocessing import Process, Queue, Event
from datetime import datetime
from sitemap_extractor import SitemapExtractor
from spider_new import run_batch_spiders_multiprocess
import time
import signal
import os

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

# Global variables for process management
active_processes = []
shutdown_event = Event()

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    print(f"\nReceived signal {signum}. Initiating graceful shutdown...")
    shutdown_event.set()
    
    # Wait for processes to finish current work
    for process in active_processes:
        if process.is_alive():
            print(f"Waiting for process {process.pid} to complete...")
            process.join(timeout=30)  # Give 30 seconds to finish
            if process.is_alive():
                print(f"Force terminating process {process.pid}")
                process.terminate()
    
    sys.exit(0)

def process_message(message_body, receipt_handle, queue_url):
    """Process a single SQS message and return job data"""
    try:
        current_date = datetime.now().strftime("%Y%m%d")
        processed_data = None

        if isinstance(message_body, str):
            message_body = json.loads(message_body)

        if 'css_pattern' in message_body:
            pattern_str = message_body['css_pattern']
            message_body['css_pattern'] = json.loads(pattern_str)
            pattern_str = None

        for key in ['sitemap_urls', 'patterns']:
            if key in message_body:
                value = message_body[key]
                if isinstance(value, str):
                    try:
                        message_body[key] = json.loads(value.strip('"').replace('\\"', '"'))
                    except json.JSONDecodeError:
                        message_body[key] = [] if key in ['sitemap_urls', 'patterns'] else {}
                    value = None

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
        s3_key_prefix = f"webscraping/dealer_output/{current_date}/{dealer_name}"

        processed_data = {
            'urls': url_list,
            'css_patterns': css_patterns,
            's3_bucket': S3_BUCKET,
            's3_key_prefix': s3_key_prefix,
            'receipt_handle': receipt_handle,
            'queue_url': queue_url,
            'dealer_name': dealer_name
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
        gc.collect()

def collect_sqs_messages(max_messages=30, max_wait_seconds=30):
    """Collect messages from SQS queue"""
    collected_messages = []
    start_time = datetime.now()
    
    while len(collected_messages) < max_messages:
        if shutdown_event.is_set():
            break
            
        try:
            response = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=min(10, max_messages - len(collected_messages)),  # AWS max is 10
                WaitTimeSeconds=5
            )
            messages = response.get('Messages', [])
            if messages:
                collected_messages.extend(messages)
                print(f"Collected {len(messages)} messages, total: {len(collected_messages)}")
            else:
                print("No messages received in this batch")
                
        except Exception as e:
            print(f"Error fetching from SQS: {e}")
            break

        elapsed = (datetime.now() - start_time).total_seconds()
        if elapsed >= max_wait_seconds:
            print("Max wait time reached while polling SQS.")
            break
    
    return collected_messages

def process_messages_batch(messages):
    """Process a batch of messages in parallel using multiprocessing"""
    if not messages:
        return []
    
    print(f"Processing {len(messages)} messages in parallel...")
    
    # Use multiprocessing Pool for message processing
    with mp.Pool(processes=min(len(messages), mp.cpu_count())) as pool:
        # Create arguments for each message
        args = [(msg['Body'], msg['ReceiptHandle'], QUEUE_URL) for msg in messages]
        
        # Process messages in parallel
        results = pool.starmap(process_message, args)
    
    # Filter out None results
    valid_jobs = [job for job in results if job is not None]
    print(f"Successfully processed {len(valid_jobs)} valid jobs from {len(messages)} messages")
    
    return valid_jobs

def run_multiprocess_spiders(dealer_jobs):
    """Run spider processes with multiprocessing using the new batch function"""
    if not dealer_jobs:
        print("No dealer jobs to process")
        return True
    
    print(f"Starting {len(dealer_jobs)} spider jobs with multiprocessing...")
    
    # Use the optimized batch processing function
    success, results = run_batch_spiders_multiprocess(dealer_jobs, max_processes=3)
    
    # Log detailed results
    successful_jobs = [r for r in results if r.get('success', False)]
    failed_jobs = [r for r in results if not r.get('success', False)]
    
    print(f"Batch Results Summary:")
    print(f"  ✓ Successful: {len(successful_jobs)}")
    print(f"  ✗ Failed: {len(failed_jobs)}")
    
    if failed_jobs:
        print("Failed jobs:")
        for job in failed_jobs:
            print(f"  - {job.get('dealer_name', 'unknown')}: {job.get('error', 'Unknown error')}")
    
    return success

def main_loop():
    """Main processing loop with multiprocessing"""
    batch_count = 0
    MAX_BATCH_SIZE = 30
    MAX_WAIT_SECONDS = 30
    
    print("Starting multiprocess consumer...")
    print(f"Configuration: MAX_BATCH_SIZE={MAX_BATCH_SIZE}, MAX_PROCESSES=3")
    
    while not shutdown_event.is_set():
        try:
            print(f"\n--- Starting Batch #{batch_count + 1} ---")
            
            # Step 1: Collect messages from SQS
            print("Step 1: Collecting messages from SQS...")
            collected_messages = collect_sqs_messages(MAX_BATCH_SIZE, MAX_WAIT_SECONDS)
            
            if not collected_messages:
                print("No more messages in SQS queue. Stopping...")
                break
            
            print(f"Collected {len(collected_messages)} messages from SQS")
            
            # Step 2: Process messages in parallel
            print("Step 2: Processing messages in parallel...")
            dealer_jobs = process_messages_batch(collected_messages)
            
            if not dealer_jobs:
                print("No valid jobs in this batch, moving to next.")
                continue
            
            # Step 3: Run spiders with multiprocessing
            print("Step 3: Running spiders with multiprocessing...")
            batch_success = run_multiprocess_spiders(dealer_jobs)
            
            if batch_success:
                print(f"Batch #{batch_count + 1} completed successfully!")
            else:
                print(f"Batch #{batch_count + 1} completed with some failures")
            
            batch_count += 1
            
            # Small delay between batches
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("\nKeyboard interrupt received. Shutting down...")
            shutdown_event.set()
            break
        except Exception as e:
            print(f"Error in main loop: {e}")
            traceback.print_exc()
            # Continue with next batch
            continue
        finally:
            gc.collect()
    
    # print("Main loop finished")

if __name__ == "__main__":
    import atexit

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Set multiprocessing start method
    if hasattr(mp, 'set_start_method'):
        try:
            mp.set_start_method('spawn', force=True)
        except RuntimeError:
            pass  # Start method already set

    def cleanup():
        print("Cleaning up resources...")

        # Close the boto3 SQS client if possible
        try:
            if hasattr(sqs, 'close'):
                sqs.close()
                print("SQS client closed.")
        except Exception as e:
            print(f"Error closing SQS client: {e}")

        # Terminate any lingering active processes
        for process in active_processes:
            if process.is_alive():
                print(f"Force terminating lingering process {process.pid}")
                process.terminate()
                process.join()

        print("All cleanup done.")

    # Register cleanup handler for normal exit
    atexit.register(cleanup)

    try:
        main_loop()
        print("Finished scraping all dealers.")
    except Exception as e:
        print(f"Fatal script error: {e}")
        traceback.print_exc()
        sys.exit(1)

    # Final hard exit if the process still hangs
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
