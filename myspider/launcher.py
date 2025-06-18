# launcher.py
import boto3, json, gc
from datetime import datetime
from sitemap_extractor import SitemapExtractor
from worker import run_spider_job
from multiprocessing import Pool

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/884203033942/webscraping'
S3_BUCKET = 'rapidious-datalake'
sqs = boto3.client('sqs', region_name='us-east-1')

def process_msg(raw):
    msg = json.loads(raw['Body'])
    if 'css_pattern' in msg:
        msg['css_pattern']=json.loads(msg['css_pattern'])
    for k in ['sitemap_urls','patterns']:
        if k in msg and isinstance(msg[k],str):
            try:
                msg[k]=json.loads(msg[k])
            except:
                msg[k]=[]
    urls = SitemapExtractor().get_filtered_urls_from_source(msg)
    if not urls:
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=raw['ReceiptHandle'])
        return None
    return {
        'urls':[u['dp_url'] for u in urls],
        'css_patterns':msg['css_pattern'],
        's3_bucket':S3_BUCKET,
        's3_key_prefix':f"webscraping/dealer_output/{datetime.now():%Y%m%d}/{msg.get('dealer_name','unknown')}",
        'receipt_handle':raw['ReceiptHandle'],
        'queue_url':QUEUE_URL
    }

def poll_and_run(batch_count=5, max_parallel=5):
    msgs = sqs.receive_message(QueueUrl=QUEUE_URL,MaxNumberOfMessages=batch_count,WaitTimeSeconds=10).get('Messages',[])
    if not msgs:
        print("No messages - exiting.")
        return
    jobs = []

    for m in msgs:
        result = process_msg(m)
        if result:
            jobs.append(result)
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=m['ReceiptHandle'])
        
    jobs = [j for j in jobs if j]
    gc.collect()

    with Pool(processes=max_parallel) as pool:
        pool.map(run_spider_job, jobs)

if __name__=="__main__":
    poll_and_run(batch_count=15, max_parallel=5)
