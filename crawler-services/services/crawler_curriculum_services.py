import os
import hashlib
import requests
import duckdb
import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from services.crawler_base_services import CrawlerBaseServices

class CrawlerCurriculumServices(CrawlerBaseServices):
    def __init__(self):
        super().__init__()
        self.save_dir = "F:/data_lake/raw_data"
        
        
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
        

    def save_pdf_to_lake(self, program_name, parent_url, file_url):
        today = datetime.now().strftime('%Y-%m-%d')
        base_path = os.path.join(self.save_dir, f"{today}/programs/{program_name}")
        pdfs_path = os.path.join(base_path, "pdfs/")
        
        os.makedirs(pdfs_path, exist_ok=True)
        entry = self._get_manifest_template()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36',
            'Accept': 'application/pdf,application/xhtml+xml,xml;q=0.9,*/*;q=0.8'
        }
        
        status = "Pending"
        hash_pdf = None
        full_file_path = None
        final_pdf_url = file_url
        hash_url = hashlib.md5(file_url.encode('utf-8')).hexdigest()
        
        try:
            response = requests.get(file_url, headers=headers, timeout=15, allow_redirects=True)
            final_pdf_url = response.url
            
            hash_url = hashlib.md5(final_pdf_url.encode('utf-8')).hexdigest()
            full_file_path = os.path.join(pdfs_path, f"{hash_url}.pdf")
            
            if response.status_code == 200:
                with open(full_file_path, "wb") as f:
                    f.write(response.content)
                
                hash_pdf = hashlib.md5(response.content).hexdigest()
                status = "Success"
                
            else:
                status = f"Failed: HTTP {response.status_code}"
                full_file_path = None
                
        except Exception as e:
            print(f"[ERROR][save_pdf_to_lake] Failed to download {file_url}: {e}")
            status = f"Failed: {str(e)}"
            
            
        entry.update({
            "hash_url": hash_url,
            "file_type": "pdf",
            "program_category": program_name,
            "source_url": file_url,
            "final_url": final_pdf_url,
            "parent_page_url": parent_url,
            "local_path": full_file_path,
            "hash_content": hash_pdf,
            "timestamp": datetime.now(),
            "status": status
        })
                    
        return entry
    
    
    def save_html_to_lake(self, program_name, url):
        today = datetime.now().strftime('%Y-%m-%d')
        base_path = os.path.join(self.save_dir, f"{today}/programs/{program_name}")
        html_path = os.path.join(base_path, "index.html")
        os.makedirs(base_path, exist_ok=True)
        
        entry = self._get_manifest_template()
        
        # Convert the url into hash value
        hash_url = hashlib.md5(url.encode('utf-8')).hexdigest()
        hash_html = None
        
        try:
            self.driver.get(url)

            # Get the raw HTML
            raw_html = self.driver.page_source
            
            # Save the original content
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(raw_html)
                
            # Convert the content into hash value
            hash_html = hashlib.md5(raw_html.encode('utf-8')).hexdigest()
    
            status = "Success"
            
        except Exception as e:
            print(f"[ERROR][save_html_to_lake] Failed to crawl {url}: {e}")
            status = f"Failed: {str(e)}"
    
        entry.update({
            "hash_url": hash_url,
            "file_type": "html",
            "program_category": program_name,
            "source_url": url,
            "final_url": url,
            "local_path": html_path if status == "Success" else None,
            "hash_content": hash_html,
            "timestamp": datetime.now(),
            "status": status
        })
        
        return entry
    
    
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
        """
            Chỉ viết 1 hàm này, (do nó là abstract method)
            Có thế viết nhiều hàm helper hỗ trợ cho hàm này.
        """
        today = datetime.now().strftime('%Y-%m-%d')
        self.driver.get(url)
        
        # Get all the URLs that navigate to the education programs
        program_urls = [url.get_attribute("href") for url in self.driver.find_elements(By.CSS_SELECTOR, ".column_attr a")]
        manifest_path = os.path.join(self.save_dir, "manifest.parquet")
        manifest_data = []
        
        for p_url in program_urls:
            if not p_url:
                continue
            
            program_name = p_url.strip('/').split('/')[-1]
            html_entry = self.save_html_to_lake(program_name, p_url)
            
            # Check HTML
            if self._is_already_crawled(p_url, html_entry['hash_content'], manifest_path):
                print(f"[SKIP] HTML for {p_url} is unchanged.")
                
                if html_entry['local_path'] and os.path.exists(html_entry['local_path']):
                    os.remove(html_entry['local_path'])
                continue
            
            manifest_data.append(html_entry)
            
            
            # Check PDF
            pdf_elements = self.driver.find_elements(By.CSS_SELECTOR, "li.pdf a")
            for element in pdf_elements:
                pdf_url = element.get_attribute("href")
                if not pdf_url:
                    continue
                
                pdf_entry = self.save_pdf_to_lake(program_name, p_url, pdf_url)
                
                if self._is_already_crawled(pdf_url, pdf_entry['hash_content'], manifest_path):
                    if pdf_entry['local_path'] and os.path.exists(pdf_entry['local_path']):
                        os.remove(pdf_entry['local_path'])
                else:
                    manifest_data.append(pdf_entry)

        if manifest_data:
            df_new = pd.DataFrame(manifest_data)
            
            if os.path.exists(manifest_path):
                df_old = pd.read_parquet(manifest_path)
                df_final = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df_final = df_new
                
            os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
            df_final.to_parquet(manifest_path, index=False)
            
        print("Curriculum Crawler")