from abc import ABC, abstractmethod
from playwright.async_api import async_playwright
import hashlib
import json
import httpx
from pathlib import Path
from datetime import datetime, timezone
from typing import Any
import logging
import asyncio
import mimetypes


# ================================================== #
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_NETWORK_IDLE_TIMEOUT_MS = 5000
# ================================================== #

logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    SITE_NAME: str
    
    def __init__(
        self,
        root_dir: Path,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        network_idle: float = DEFAULT_NETWORK_IDLE_TIMEOUT_MS,
    ):
        self.root_dir = root_dir
        self.user_agent = user_agent
        self.timeout = timeout * 1000 # milliseconds
        self.network_idle = network_idle
        self.session: httpx.AsyncClient | None
        
        self._cache_lock = asyncio.Lock()
        
        self._started_at = datetime.now().strftime("%Y%m%dT%H%M")
        
        cache_dir = self.root_dir / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_path = cache_dir / "scraper_cache.json"
        
        self.root_dir.mkdir(parents=True, exist_ok=True)
        
        if not self.cache_path.exists():
            self.cache_path.write_text("{}", encoding="utf-8")
        
        
    
    registry: dict[str, type['BaseScraper']] = {}
    def __init_subclass__(cls):
        super().__init_subclass__()
        if hasattr(cls, "SITE_NAME"):
            BaseScraper.registry[cls.SITE_NAME] = cls
    
        
        
    async def __aenter__(self):
        self.session = httpx.AsyncClient(
            follow_redirects=True,
        )
        self._playwright = await async_playwright().start()

        self.browser = await self._playwright.chromium.launch(
            headless=True
        )

        self.context = await self.browser.new_context(
            user_agent=self.user_agent
        )
        self.page = await self.context.new_page()

        return self
    
    
    
    async def __aexit__(self, exc_type, exc, tb):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if getattr(self, "_playwright", None):
            await self._playwright.stop()
        if self.session:
            await self.session.aclose()
    
    
    
    def _make_site_directory(self) -> Path:
        site_dir = self.root_dir / "raw" / self.SITE_NAME
        site_dir.mkdir(parents=True, exist_ok=True)
        return site_dir



    def _load_cache(self) -> dict[str, Any]:
        try:
            return json.loads(
                self.cache_path.read_text(encoding="utf-8")
            )
        except (json.JSONDecodeError, OSError):
            logger.warning("Cache.json is invalid; resetting it")
            return {}



    def _save_cache(self, cache_data: dict[str, Any]) -> None:
        self.cache_path.write_text(
            json.dumps(cache_data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )



    def _storage_path(self, target_path: Path) -> str:
        """
        Store paths relative to root_dir.

        Example:
            raw/curriculum/abc123.pdf
        """
        return str(target_path.relative_to(self.root_dir))
    
    
    
    async def _store_raw_document(
        self,
        content: bytes,
        extension: str,
        filename: str | None = None,
    ) -> dict[str, Any]:
        """
        Store a raw document under data/raw/<SITE_NAME>/ using its content hash.

        Returns:
            {
                "filename": "...",
                "file_path": "...",
                "size_bytes": 1234,
                "file_hash": "...",
            }
        """
        raw_dir = self._make_site_directory()

        if not extension.startswith("."):
            extension = f".{extension}"

        file_hash = hashlib.sha256(content).hexdigest()[:16]

        target_path = raw_dir / f"{file_hash}{extension}"

        if not target_path.exists():
            target_path.write_bytes(content)

            logger.debug(
                "Saved raw document: %s",
                target_path,
            )
        else:
            logger.debug(
                "Raw document already exists: %s",
                target_path,
            )

        return {
            "filename": filename or target_path.name,
            "file_path": self._storage_path(target_path),
            "size_bytes": len(content),
            "file_hash": file_hash,
        }
    
    
    
    async def _cache_document(
        self,
        *,
        cache_key: str,
        content: bytes,
        extension: str,
        filename: str | None = None,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        async with self._cache_lock:
            cache = self._load_cache()

            cached = cache.get(cache_key)

            if cached:
                local_path = self.root_dir / cached["local_path"]

                if local_path.is_file():
                    logger.debug(
                        "Reusing cached document: %s",
                        cache_key,
                    )

                    return {
                        "filename": cached["filename"],
                        "file_path": cached["local_path"],
                        "size_bytes": cached["size_bytes"],
                        "file_hash": cached["file_hash"],
                    }

        metadata = await self._store_raw_document(
            content=content,
            extension=extension,
            filename=filename,
        )

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        async with self._cache_lock:
            cache = self._load_cache()

            cache[cache_key] = {
                "file_hash": metadata["file_hash"],
                "local_path": metadata["file_path"],
                "filename": metadata["filename"],
                "size_bytes": metadata["size_bytes"],
                "content_type": content_type,
                "downloaded_at": cache.get(cache_key, {}).get(
                    "downloaded_at",
                    now,
                ),
                "last_seen": now,
            }

            self._save_cache(cache)

        return metadata
    
    
    
    async def _download_attachment(
        self,
        file_url: str,
        file_name: str | None = None,
    ) -> dict[str, Any] | None:
        if not self.session:
            raise RuntimeError("HTTP client is not initialized.")

        try:
            response = await self.session.get(file_url)
            response.raise_for_status()

            extension = (
                Path(file_name or Path(file_url).name).suffix
                or mimetypes.guess_extension(
                    response.headers.get("Content-Type", "").split(";")[0]
                )
                or ".bin"
            )

            return await self._cache_document(
                cache_key=file_url,
                content=response.content,
                extension=extension,
                filename=file_name,
                content_type=response.headers.get("Content-Type"),
            )

        except Exception as exc:
            logger.warning(
                "Failed to download attachment '%s': %s",
                file_url,
                exc,
            )
            return None
    
    
    
    @abstractmethod
    async def scrape(self):
        ...