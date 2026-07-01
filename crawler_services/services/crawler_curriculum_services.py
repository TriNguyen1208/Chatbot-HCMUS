from selenium.webdriver.common.by import By
from services.crawler_base_services import CrawlerBaseServices
from constant.type import PageType, URLType
from typing import override
from bs4 import BeautifulSoup


class CrawlerCurriculumServices(CrawlerBaseServices):
    def __init__(self):
        super().__init__()
    
    @override
    def crawl(self, url= "https://www.ctda.hcmus.edu.vn/vi/educational-program/") -> str:
        self.driver.get(url)
        
        # Get all the URLs that navigate to the education programs
        program_urls = [url.get_attribute("href") for url in self.driver.find_elements(By.CSS_SELECTOR, ".column_attr a")]
        
        for p_url in program_urls:
            if not p_url:
                continue
            
            html_result = self.get_html(PageType.CURRICULUM, p_url)
            
            if "Failed" in html_result['status']:
                continue
            
            if self.db.is_url_existed(p_url, html_result['hash_content']):
                print(f'[INFO][crawl][html]: "{p_url}" has already crawled')
                continue
            
            self.db.add_lake(**html_result)
                
            pdf_elements = self.driver_content.find_elements(By.CSS_SELECTOR, "li.pdf a")
            for element in pdf_elements:
                pdf_url = element.get_attribute("href")
                
                if not pdf_url:
                    continue
                
                pdf_result = self.get_pdf(PageType.CURRICULUM, pdf_url)
                if "Failed" in pdf_result["status"]:
                    continue
                
                if not self.db.is_url_existed(pdf_result['url'], pdf_result['hash_content']):
                    self.db.add_lake(**pdf_result)
                else:
                    print(f'[INFO][crawl][pdf]: "{pdf_result['url']}" has already crawled')

    @override
    def preprocess(self):
        query_result = self.db.get_unprocessed_data(page_type=PageType.CURRICULUM, url_type=URLType.HTML)
        
        if not query_result:
            return
        
        for row in query_result:
            html_url = row.get('url')
            lake_id = row.get('id')
            
            try: 
                self.driver_content.get(html_url)
                raw_html = self.driver_content.page_source
            
                if not raw_html:
                    print(f"[WARNING][preprocess]: Could not retrieve HTML for url {html_url}")
                    continue
            
                soup = BeautifulSoup(raw_html, 'html.parser')
                for element in soup.select("script, style, nav, footer, header, aside, em, .download-attachments"):
                    element.decompose()
                    
                raw_text = soup.get_text(separator=' ')
                lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
                cleaned_text = '\n'.join(lines)
                
                if cleaned_text:
                    self.db.add_text_warehouse(row['id'], cleaned_text)
                
            except Exception as e:
                print(f"[ERROR][preprocess]: Failed to process URL {html_url}: {e}")
                continue