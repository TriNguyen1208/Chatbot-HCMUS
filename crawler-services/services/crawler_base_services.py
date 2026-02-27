from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from db.db import Database
import requests
from datetime import datetime
from utils.utils import generate_hash_content
from constant.type import URLType

class CrawlerBaseServices(ABC):
    def __init__(self):
        super().__init__()
        self.option = Options()
        self.option.add_argument("--headless")
        self.driver = webdriver.Chrome(options=self.option)
        self.driver_content = webdriver.Chrome(options=self.option)
        self.db = Database()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'application/pdf,application/xhtml+xml,xml;q=0.9,*/*;q=0.8'
        }
        self.allowed_domains_htmls = (
            "https://hcmus.edu.vn",
            "https://www.ctda.hcmus.edu.vn",
        )

        
    @abstractmethod
    def crawl(self) -> str:
        """
            Crawl all the contents in url until all the urls are visited.
        """
        pass

    def close(self):
        if self.driver:
            self.driver.quit()
            print("Close driver successfully")
            
        if self.driver_content:
            self.driver_content.quit()
            print("Close driver_content successfully")

    def get_pdf(self, page_type, url_pdf):
        '''
            Navigates to the URL, gets the raw HTML PDF source
            
            Args:
                url(str): The specific program URL to crawl
                
            Returns:
                dict: A result summary containing:
                    - url (str): The processed URL.
                    - hash_content (str|None): MD5 hash of the HTML (None if the crawl failed).
                    - status (str): Execution state ("Success" or "Failed: {error_message}").
                    - timestamp (datetime): The exact time the operation occurred.
        '''
            
        hash_pdf = None
        try:
            response = requests.get(url_pdf, headers=self.headers, timeout=15, allow_redirects=True)
            final_pdf_url = response.url
            
            if response.status_code == 200:
                hash_pdf = generate_hash_content(response.content)
                status = "Success"
                
            else:
                print(f"[WARNING][add_pdf_to_lake] HTTP code when processing {url_pdf}: {response.status_code}")
                status = f"Failed: HTTP {response.status_code}"
        except Exception as e:
            print(f"[WARNING][add_pdf_to_lake] Failed to download content of {url_pdf}: {e}")
            status = f"Failed: {str(e)}"
        result = {
            "url": final_pdf_url,
            "page_type": page_type,
            "hash_content": hash_pdf,
            "url_type": URLType.PDF,
            "status": status,
        }
        return result

    def get_html(self, page_type, url_html):
        '''
            Navigates to the URL, gets the raw HTML source, and adds the metadata to Data Lake
            
            Args:
                url(str): The specific program URL to crawl
                
            Returns:
                dict: A result summary containing:
                    - url (str): The processed URL.
                    - hash_content (str|None): MD5 hash of the HTML (None if the crawl failed).
                    - status (str): Execution state ("Success" or "Failed: {error_message}").
                    - timestamp (datetime): The exact time the operation occurred.
        '''
        raw_html = None
        hash_html = None
        
        try:
            self.driver_content.get(url_html)
            # Get the raw HTML
            raw_html = self.driver_content.page_source
            hash_html = generate_hash_content(raw_html)
            status = "Success"
            
        except Exception as e:
            print(f"[WARNING][add_html_to_lake] Failed to crawl {url_html}: {e}")
            status = f"Failed: {str(e)}"
        
        result = {
            "url": url_html,
            "page_type": page_type,
            "hash_content": hash_html,
            "url_type": URLType.HTML,
            "status": status,
        }
        
        return result

    def check_url_type(self, url: str):
        if url.split(".")[-1] == 'pdf':
            return URLType.PDF
        elif "docs" in url:
            return URLType.DOCS
        elif any(domain in url for domain in self.allowed_domains_htmls):
            return URLType.HTML
        return URLType.UNKNOWN