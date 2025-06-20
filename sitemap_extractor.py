import requests
import logging
import random
import re
from xml.etree import ElementTree as ET
from typing import List, Dict, Optional
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class SitemapExtractor:
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
    ]
    YEAR_PATTERN = re.compile(r'\b(19[4-9]\d|20[0-1]\d|202[0-6])\b')

    def __init__(self, max_retries: int = 2, timeout: int = 20):
        self.timeout = timeout
        
        # Setup requests session with retries
        self.session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
    def _get_random_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': random.choice(self.USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }

    def fetch(self, url: str) -> Optional[str]:
        headers = self._get_random_headers()
        try:
            response = self.session.get(url, headers=headers, timeout=self.timeout)
            if response.status_code == 200:
                return response.text
            logging.warning(f"Non-200 status code: {response.status_code} for URL: {url}")
        except Exception as e:
            logging.error(f"Fetch failed for {url}: {e}")
        return None

    def _check_year_in_url(self, url: str) -> bool:
        return self.YEAR_PATTERN.search(url) is not None

    def parse_sitemap(self, source_url: str, sitemap_url: str, 
                     content: str, patterns: List[str]) -> List[Dict[str, str]]:
        try:
            root = ET.fromstring(content)
            urls = [elem.text for elem in root.iter() 
                   if elem.tag.endswith('loc') and elem.text]
            return self._filter_urls(urls, patterns)
        except ET.ParseError as e:
            logging.error(f"Parse error for {sitemap_url}: {e}")
            return []

    def _filter_urls(self, urls: List[str], patterns: List[str]) -> List[Dict[str, str]]:
        filtered = []
        for url in urls:
            if self._should_include_url(url, patterns):
                filtered.append({"dp_url": url})
        return filtered

    def _has_alphanumeric_pattern(self, url: str) -> bool:
        # Match patterns like "22-idle" or "241rks" or "274vfkbl"
        return bool(re.search(r'[0-9]+[a-zA-Z]+[0-9a-zA-Z-]*', url))
    
    def _starts_with_number(self, url: str) -> bool:
        # Look specifically for numbers between slashes
        return bool(re.search(r'/\d+/', url))

    def _is_image_url(self, url: str) -> bool:
        """Check if URL is likely an image link"""
        image_patterns = [
            r'\.(jpg|jpeg|png|gif|webp|svg)(\?|$)',  # Common image extensions
            r'/images?/',         # Common image path segments
            r'/photos?/',         # Photo directories
            r'/thumbnails?/',     # Thumbnail directories
            r'/gallery/',         # Gallery paths
            r'/media/',           # Media paths
            r'[\-_]photo[\-_]',   # URL segments containing 'photo'
            r'/assets/',          # Asset directories
            r'/pictures?/'        # Picture directories
        ]
        return any(re.search(pattern, url.lower()) for pattern in image_patterns)

    def _should_include_url(self, url: str, patterns: List[str]) -> bool:
        # First check for explicitly excluded patterns
        if any([
            "?trimid" in url,
            "Automobiles" in url,
            self._is_image_url(url)
        ]):
            return False

        if not patterns:
            return True
            
        return any(
            (pattern == "startswithyear" and self._check_year_in_url(url)) or
            (pattern == "containsalphanumeric" and self._has_alphanumeric_pattern(url)) or
            (pattern == "startswithnumber" and self._starts_with_number(url)) or
            pattern in url
            for pattern in patterns
        )

    def get_filtered_urls_from_source(self, source: Dict) -> List[Dict[str, str]]:
        try:
            if not isinstance(source.get('sitemap_urls'), list):
                raise ValueError("sitemap_urls must be a list")
            if not isinstance(source.get('patterns'), list):
                raise ValueError("patterns must be a list")
            
            all_urls = []
            for sitemap_url in source["sitemap_urls"]:
                content = self.fetch(sitemap_url)
                if content:
                    urls = self.parse_sitemap(
                        source["source_url"], 
                        sitemap_url, 
                        content, 
                        source["patterns"]
                    )
                    all_urls.extend(urls)
            return all_urls
            
        except Exception as e:
            logging.error(f"Error processing source {source.get('source_url', 'unknown')}: {str(e)}")
            return []