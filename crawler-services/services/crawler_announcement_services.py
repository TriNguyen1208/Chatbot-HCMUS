from services.crawler_base_services import CrawlerBaseServices
from selenium.webdriver.common.by import By

class CrawlerAnnouncementServices(CrawlerBaseServices):
    def __init__(self):
        super().__init__()
        #Có thể thêm gì tuỳ ý.

    def crawl(self, url= "https://hcmus.edu.vn/category/dao-tao/dai-hoc/thong-tin-danh-cho-sinh-vien/") -> str:
        """
            Chỉ viết 1 hàm này, (do nó là abstract method)
            Có thế viết nhiều hàm helper hỗ trợ cho hàm này.
        """
        print("Announcement Crawler")
        pass
