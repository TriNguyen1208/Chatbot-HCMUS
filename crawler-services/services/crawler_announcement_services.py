from services.crawler_base_services import CrawlerBaseServices
from selenium.webdriver.common.by import By
from typing import override
from selenium import webdriver
from constant.type import Type

class CrawlerAnnouncementServices(CrawlerBaseServices):
    def __init__(self):
        super().__init__()
        #Có thể thêm gì tuỳ ý.
        self.urls = []
        self.driver_content = webdriver.Chrome(options=self.option)

    def __remove_attribute(self, element, attribute):
        for attri in element.find_elements(By.TAG_NAME, attribute):
            self.driver_content.execute_script('''
                var element = arguments[0];
                element.parentNode.removeChild(element);
            ''', attri)

    def __remove_last_line(self, element):
        self.driver_content.execute_script("""
            var el = arguments[0];
            var divs = el.getElementsByTagName('div');
            if (divs.length > 0) {
                divs[divs.length - 1].remove();
            }
        """, element)

    def get_all_url(self, url_base) -> str:
        #Lấy số lượng trang
        self.driver.get(url=url_base)
        element_ul_page_numbers = self.driver.find_element(
            By.CSS_SELECTOR, "ul.page-numbers"
        )
        num_pages = max(
            int(a.text)
            for a in element_ul_page_numbers.find_elements(By.TAG_NAME, "a")
            if a.text.isdigit()
        )
        
        #Dùng vòng lặp for để lặp số lượng trang
        for i in range(1, num_pages + 1):
            #Lấy nội dung của trang thứ i
            url = f'https://hcmus.edu.vn/category/dao-tao/dai-hoc/thong-tin-danh-cho-sinh-vien/page/{i}'
            self.driver.get(url)

            #Lấy tất cả articles của trang thứ i
            elements_articles = self.driver.find_elements(By.TAG_NAME, 'article')

            #Duyệt từng articles
            for element in elements_articles:
                #Lấy tất cả url của articles
                element_url_article = element.find_element(By.TAG_NAME, 'a')
                url_article = element_url_article.get_attribute('href')
                self.db.add_lake(
                    url=url_article,
                    type=Type.ANNOUNCEMENT,
                    status="Success"
                )
                
                #Lấy tất cả url bên trong articles đó
                self.driver_content.get(url_article)
                element_content = self.driver_content.find_element(By.CSS_SELECTOR, '.cmsmasters_post_content.entry-content')
                links = element_content.find_elements(By.TAG_NAME, 'a')
                for link in links:
                    href = link.get_attribute('href')
                    self.db.add_lake(
                        url=href,
                        type=Type.ANNOUNCEMENT,
                        status="Success"
                    )
    @override
    def crawl(self, url= "https://hcmus.edu.vn/category/dao-tao/dai-hoc/thong-tin-danh-cho-sinh-vien/") -> str:
        """
            Chỉ viết 1 hàm này, (do nó là abstract method)
            Có thế viết nhiều hàm helper hỗ trợ cho hàm này.
        """
        # self.db.add_lake(
        #     url=url,
        #     type=Type.ANNOUNCEMENT,
        #     hash_content="Huhu haha",
        #     status="Done"
        # )
        
        # self.db.add_text_warehouse(
        #     lake_id=1,
        #     content="haha"
        # )
        
        # self.db.add_sheet_records_warehouse(
        #     lake_id=1,
        #     url=url,
        #     table_name="Haha"
        # )
        #Dựa vào url là page (phân trang lấy ra lưu vào trong biến)
        #Sau đó ở từng trang lấy ra thẻ a trong article
        #Lưu hết vào trong biến
        #Đi vào thẻ a đó nếu như trong article có thẻ a nữa thì lưu vào biến đó
        
        url= "https://hcmus.edu.vn/category/dao-tao/dai-hoc/thong-tin-danh-cho-sinh-vien/"

        # Get all urls
        self.get_all_url(url_base=url)
