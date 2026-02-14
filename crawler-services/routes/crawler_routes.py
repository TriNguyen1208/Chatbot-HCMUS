from fastapi import APIRouter
from controllers.crawler_controller import CrawlerController

router = APIRouter()
controller = CrawlerController()

@router.get("/crawl")
def run():
    return controller.run()
