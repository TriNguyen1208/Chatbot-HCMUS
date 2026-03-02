from factory.crawler_factory import CrawlerFactory

name = "curriculum"
crawler_instance = CrawlerFactory.get_crawler(name)
try:
    print(f"[INFO] Start to crawl '{name}'")
    crawler_instance.crawl()
    print(f"[INFO] Crawl '{name}' successfully")
    
    print(f"[INFO] Start to preprocess '{name}'")
    crawler_instance.preprocess()
    print(f"[INFO] preprocess '{name}' successfully")
except Exception as e:
    print(e)
finally:
    crawler_instance.close()