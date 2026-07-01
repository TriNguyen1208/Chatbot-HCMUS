from pathlib import Path
import logging

from crawler_services.pipeline.scraping.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class ScraperManager:
    def __init__(self, root_dir: Path):
        self.root_dir = root_dir

    async def scrape(self, site_name: str):
        scraper_cls = BaseScraper.registry.get(site_name)
        
        if scraper_cls is None:
            logger.error(
                "Unknown scraper: %s",
                site_name,
            )
            return

        logger.info(
            "Starting scraper: %s",
            site_name,
        )

        try:
            async with scraper_cls(self.root_dir) as scraper:
                await scraper.scrape()

            logger.info(
                "Finished scraper: %s",
                site_name,
            )

        except Exception:
            logger.exception(
                "Scraper '%s' failed",
                site_name,
            )

    async def scrape_all(self) -> None:
        logger.info(
            "Starting %d scraper(s)",
            len(BaseScraper.registry),
        )

        for site_name, scraper_cls in BaseScraper.registry.items():
            logger.info(
                "Running scraper: %s",
                site_name,
            )

            try:
                async with scraper_cls(self.root_dir) as scraper:
                    await scraper.scrape()

                logger.info(
                    "Completed scraper: %s",
                    site_name,
                )

            except Exception:
                logger.exception(
                    "Scraper '%s' failed",
                    site_name,
                )

        logger.info("Finished all scrapers")