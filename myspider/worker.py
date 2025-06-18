# worker.py
import scrapy
import csv, io, re, logging
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.http import Request, HtmlResponse
from sitemap_extractor import SitemapExtractor
import boto3
from dataclasses import dataclass
from typing import Optional, List

logging.getLogger('scrapy').setLevel(logging.WARNING)

@dataclass
class ScrapingPattern:
    selector: List[str]
    attr: Optional[str] = None
    multiple: bool = False
    count: bool = False
    regex: Optional[str] = None

class DealerSpider(scrapy.Spider):
    name = "dealer"
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0',
        'DOWNLOAD_DELAY': 0.5,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 2,
        'FEEDS': {}  # no need, we upload manually
    }

    def __init__(self, job=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.urls = job['urls']
        self.patterns = {
            k: ScrapingPattern(**v) for k,v in job['css_patterns'].items()
        }
        self.s3_bucket = job.get('s3_bucket')
        self.s3_key_prefix = job.get('s3_key_prefix')
        self.receipt_handle = job.get('receipt_handle')
        self.queue_url = job.get('queue_url')
        self.items = []

    def start_requests(self):
        for url in self.urls:
            yield Request(url, callback=self.parse, dont_filter=True)

    def parse(self, response):
        item = {'source_url': response.url}
        sel = response.selector
        for field, p in self.patterns.items():
            results = []
            for sel_str in p.selector:
                els = sel.css(sel_str)
                if p.count:
                    results = [str(len(els))]
                elif p.multiple:
                    for el in els:
                        txt = el.css(f"::text").get() if p.attr=='text' else el.attrib.get(p.attr) if p.attr else el.css("*::text").get()
                        if txt and p.regex:
                            m = re.search(p.regex, txt)
                            txt = m.group(1) if m and m.lastindex else (m.group(0) if m else '')
                        if txt: results.append(txt.strip())
                else:
                    if els:
                        el = els[0]
                        txt = el.css(f"::text").get() if p.attr=='text' else el.attrib.get(p.attr) if p.attr else el.css("*::text").get()
                        if txt and p.regex:
                            m = re.search(p.regex, txt)
                            txt = m.group(1) if m and m.lastindex else (m.group(0) if m else '')
                        if txt:
                            results = [txt.strip()]
                if results and not p.multiple: break
            item[field] = "|".join(results) if p.multiple else (results[0] if results else '')
        self.items.append(item)

    def closed(self, reason):
        if not self.items:
            return
        # Save CSV
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=['source_url']+list(self.patterns.keys()))
        writer.writeheader()
        writer.writerows(self.items)
        data = buffer.getvalue().encode('utf-8')
        s3 = boto3.client('s3',region_name='us-east-1')
        key = f"{self.s3_key_prefix}.csv"
        s3.put_object(Bucket=self.s3_bucket, Key=key, Body=data, ContentType='text/csv')
        s3.head_object(Bucket=self.s3_bucket, Key=key)
        # Delete SQS message
        if self.queue_url and self.receipt_handle:
            boto3.client('sqs', region_name='us-east-1').delete_message(
                QueueUrl=self.queue_url, ReceiptHandle=self.receipt_handle
            )

def run_spider_job(job):
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(DealerSpider, job=job)
    process.start()
