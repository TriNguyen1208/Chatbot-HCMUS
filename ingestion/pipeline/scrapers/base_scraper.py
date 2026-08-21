from abc import ABC, abstractmethod
from playwright.async_api import async_playwright
import httpx
from datetime import datetime
import logging
from typing import AsyncIterator

from ingestion.config.settings import settings

# ================================================== #
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
# ================================================== #

logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    SITE_NAME: str

    def __init__(
        self,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout: float = settings.CRAWLER_TIMEOUT_SECONDS,
        network_idle: float = settings.CRAWLER_NETWORK_IDLE_TIMEOUT_MS,
    ):
        self.user_agent = user_agent
        self.timeout = timeout * 1000  # milliseconds
        self.network_idle = network_idle
        self.session: httpx.AsyncClient | None = None

        self._started_at = datetime.now().strftime("%Y%m%dT%H%M")

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


    @abstractmethod
    async def scrape(self) -> AsyncIterator[tuple[str, bytes]]:
        ...
