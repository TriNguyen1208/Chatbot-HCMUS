from selenium.webdriver.common.by import By
from services.crawler_base_services import CrawlerBaseServices
from constant.type import PageType


class CrawlerCurriculumServices(CrawlerBaseServices):
    def __init__(self):
        super().__init__()
    
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
            
            self.db.add_lake(**html_result)
                
            pdf_elements = self.driver_content.find_elements(By.CSS_SELECTOR, "li.pdf a")
            for element in pdf_elements:
                pdf_url = element.get_attribute("href")
                
                if not pdf_url:
                    continue
                
                pdf_result = self.get_pdf(PageType.CURRICULUM, pdf_url)
                if "Failed" in pdf_result["status"]:
                    continue
                
                self.db.add_lake(**pdf_result)