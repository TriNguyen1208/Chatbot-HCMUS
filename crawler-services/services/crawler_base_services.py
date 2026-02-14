from abc import ABC, abstractmethod
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from db.db import Database
class CrawlerBaseServices(ABC):
    def __init__(self):
        super().__init__()
        self.option = Options()
        self.option.add_argument("--headless")
        self.driver = webdriver.Chrome(options=self.option)
        self.db = Database()
        
    @abstractmethod
    def crawl(self) -> str:
        """
            Crawl all the content in url until all the url is visited.
        """
        pass

    def close(self):
        if self.driver:
            self.driver.quit()
            print("Close driver successfully")
