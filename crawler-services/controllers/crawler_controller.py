from factory.crawler_factory import CrawlerFactory

class CrawlerController:
    def __init__(self):
        # self.crawlers_name = ["curriculum", "fit_info", "enrollment", "announcement"]
        self.crawlers_name = ["curriculum"]
        
    def run(self):
        for name in self.crawlers_name:
            crawler_instance = CrawlerFactory.get_crawler(name)
            try:
                print(f"[INFO] Start to crawl '{name}'")
                crawler_instance.crawl()
                print(f"[INFO] Crawl '{name}' successfully")
            except Exception as e:
                print(e)
            finally:
                crawler_instance.close()