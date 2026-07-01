import asyncio
from pathlib import Path
import logging

from crawler_services.logging import setup_logging
from crawler_services.pipeline import scraping
from crawler_services.pipeline.scraping import (
    BaseScraper,
    ScraperManager,
)


setup_logging(logging.WARNING)
ROOT_DIR = Path(__file__).resolve().parent.parent / "data"


async def main():
    print("Registered scrapers:")
    for name in BaseScraper.registry:
        print(f" - {name}")
        
    manager = ScraperManager(ROOT_DIR)

    # Scrape one site
    await manager.scrape("curriculum")

    # Or scrape all sites
    # await manager.scrape_all()


if __name__ == "__main__":
    asyncio.run(main())