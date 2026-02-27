from factory.crawler_factory import CrawlerFactory

class CrawlerController:
    def __init__(self):
        self.crawlers_name = ["curriculum", "fit_info", "enrollment", "announcement"]
        
    def run(self):
        for name in self.crawlers_name:
            crawler_instance = CrawlerFactory.get_crawler(name)
            try:
                crawler_instance.crawl()
                print(f"Crawl {name} successfully")
            except Exception as e:
                print(e)
            finally:
                crawler_instance.close()