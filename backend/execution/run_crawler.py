import asyncio
import logging

from logger_config import setup_logging
from pipeline.scrapers.scraper_manager import ScraperManager

setup_logging(level=logging.INFO)

logger = logging.getLogger(__name__)

async def main():
    logger.info("Starting crawler engine...")

    manager = ScraperManager()
    await manager.scrape_all()
    
    logger.info("Crawler engine run completed.")

if __name__ == "__main__":
    asyncio.run(main())