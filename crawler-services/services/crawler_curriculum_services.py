import os
import hashlib
import requests
import duckdb
import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from services.crawler_base_services import CrawlerBaseServices
from selenium.webdriver.common.by import By
from typing import override
from selenium import webdriver
from constant.type import Type
from utils.utils import generate_hash_content

class CrawlerCurriculumServices(CrawlerBaseServices):
    def __init__(self):
        super().__init__()
        
        
    def _get_manifest_template(self):
        """Standardized schema for all manifest entries."""
        return {
            "hash_url": None,
            "file_type": None,
            "program_category": None,
            "source_url": None,
            "final_url": None,
            "parent_page_url": None,
            "local_path": None,
            "hash_content": None,
            "timestamp": None,
            "status": "Pending"
        }
        

    def add_pdf_to_lake(self, file_url):
        '''
        Resolves a PDF link, get its binary content, and adds the metadata to Data Lake
        
        Args:
            file_url (str): The initial source URL of the PDF.
            
        Returns:
            dict: A result summary containing:
                - url (str): The final resolved destination URL of the PDF.
                - hash_content (str|None): MD5 hash of the PDF bytes (None if failed).
                - status (str): Execution state ("Success" or "Failed: {error_message}").
                - timestamp (datetime): The exact time the operation occurred.
        '''
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'application/pdf,application/xhtml+xml,xml;q=0.9,*/*;q=0.8'
        }
        
        hash_pdf = None
        final_pdf_url = file_url
        
        try:
            response = requests.get(file_url, headers=headers, timeout=15, allow_redirects=True)
            final_pdf_url = response.url
            
            if response.status_code == 200:
                hash_pdf = generate_hash_content(response.content)
                status = "Success"
                
            else:
                print(f"[WARNING][add_pdf_to_lake] HTTP code when processing {file_url}: {response.status_code}")
                status = f"Failed: HTTP {response.status_code}"
                
        except Exception as e:
            print(f"[WARNING][add_pdf_to_lake] Failed to download content of {file_url}: {e}")
            status = f"Failed: {str(e)}"
            
            
        self.db.add_lake(
            url=final_pdf_url,
            page_index=Type.CURRICULUM,
            url_type="pdf",
            hash_content=hash_pdf,
            status=status
        )
        
        result = {
            "url": final_pdf_url,
            "hash_content": hash_pdf,
            "status": status,
            "timestamp": datetime.now()
        }
                    
        return result
    
    
    def add_html_to_lake(self, url):
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
            self.driver.get(url)
            
            # Get the raw HTML
            raw_html = self.driver.page_source
            hash_html = generate_hash_content(raw_html)
            status = "Success"
            
        except Exception as e:
            print(f"[WARNING][add_html_to_lake] Failed to crawl {url}: {e}")
            status = f"Failed: {str(e)}"
            
        self.db.add_lake(
            url=url,
            page_index=Type.CURRICULUM,
            url_type="html",
            hash_content=hash_html,
            status=status
        )
        
        result = {
            "url": url,
            "hash_content": hash_html,
            "status": status,
            "timestamp": datetime.now()
        }
        
        return result
    
    
    def _is_already_crawled(self, url, content_hash, manifest_path):
        if not os.path.exists(manifest_path):
            return False
        
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
        
        query = f"""
            SELECT COUNT(*) 
            FROM '{manifest_path}' 
            WHERE hash_url = '{url_hash}' 
            AND hash_content = '{content_hash}'
            AND status = 'Success'
        """
        
        result = duckdb.query(query).fetchone()
        return result[0] > 0

    
    def crawl(self, url= "https://www.ctda.hcmus.edu.vn/vi/educational-program/") -> str:
        self.driver.get(url)
        
        # Get all the URLs that navigate to the education programs
        program_urls = [url.get_attribute("href") for url in self.driver.find_elements(By.CSS_SELECTOR, ".column_attr a")]
        
        for p_url in program_urls:
            if not p_url:
                continue
            
            html_result = self.add_html_to_lake(p_url)
            
            if "Failed" in html_result['status']:
                continue
                
            pdf_elements = self.driver.find_elements(By.CSS_SELECTOR, "li.pdf a")
            for element in pdf_elements:
                pdf_url = element.get_attribute("href")
                
                if not pdf_url:
                    continue
                
                pdf_result = self.add_pdf_to_lake(pdf_url)
                if "Failed" in pdf_result["status"]:
                    continue