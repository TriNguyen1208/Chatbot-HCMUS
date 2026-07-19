from pathlib import Path
import json
import logging
from datetime import datetime, timezone
from typing import Any
import asyncio

from config.settings import settings
from pipeline.downloader import DocumentDownloader
from pipeline.scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class ScraperManager:
    def __init__(self):
        self.manifest_path = settings.DATA_CACHE_DIR / "manifest.json"
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.downloader = DocumentDownloader()
        
        self._lock = asyncio.Lock()
        self._download_semaphore  = asyncio.Semaphore(settings.MAX_CONCURRENT_DOWNLOADS)

        if not self.manifest_path.exists():
            self.manifest_path.write_text("[]", encoding="utf-8")


    async def _process_single_file(self, filename: str, file_bytes: bytes, site_name: str):
        """Helper worker to process an individual file concurrently."""
        async with self._download_semaphore:
            try:
                metadata = await self.downloader.store_document(filename, file_bytes, site_name)
                
                async with self._lock:
                    self._append_manifest(site_name, metadata)
            except Exception as e:
                logger.error(f"Failed processing file {filename}: {str(e)}")
            
    
    async def scrape(self, site_name: str):
        scraper_cls = BaseScraper.registry.get(site_name)
        
        if scraper_cls is None:
            logger.error("Unknown scraper: %s", site_name)
            return

        logger.info("Starting scraper: %s", site_name)

        try:
            async with scraper_cls() as scraper:
                tasks = []
                
                async for filename, file_bytes in scraper.scrape():
                    task = asyncio.create_task(
                        self._process_single_file(filename, file_bytes, site_name)
                    )
                    tasks.append(task)
                
                if tasks:
                    await asyncio.gather(*tasks)

            logger.info("Finished scraper: %s", site_name)
            
        except Exception:
            logger.exception("Scraper '%s' failed", site_name)


    async def scrape_all(self) -> None:
        logger.info(
            "Starting %d scraper(s)",
            len(BaseScraper.registry),
        )
        
        for site_name in BaseScraper.registry.keys():
            await self.scrape(site_name)

        logger.info("Finished all scrapers")


    def _append_manifest(self, site_name: str, metadata: dict[str, Any]) -> None:
        try:
            manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            manifest = []

        if not isinstance(manifest, list):
            manifest = []

        if any(item.get("file_hash") == metadata["file_hash"] for item in manifest):
            logger.debug("Skipping manifest log: File version already documented.")
            return
        
        manifest.append(
            {
                "site_name": site_name,
                "file_name": metadata["file_name"],
                "file_path": metadata["file_path"],
                "file_size": metadata["file_size"],
                "file_hash": metadata["file_hash"],
                "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )

        self.manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
