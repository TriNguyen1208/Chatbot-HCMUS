from factory.crawler_factory import CrawlerFactory

name = "announcement"
crawler_instance = CrawlerFactory.get_crawler(name)
try:
    print(f"[INFO] Start to crawl '{name}'")
    crawler_instance.crawl()
    print(f"[INFO] Crawl '{name}' successfully")
except Exception as e:
    print(e)
finally:
    crawler_instance.close()