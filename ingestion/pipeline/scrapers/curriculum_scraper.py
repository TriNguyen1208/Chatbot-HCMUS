import asyncio
import mimetypes
from pathlib import Path
import logging
from typing import AsyncIterator, List
from urllib.parse import urlparse, unquote

from ingestion.pipeline.scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class CurriculumScraper(BaseScraper):
    SITE_NAME = "curriculum"
    BASE_URL = "https://www.ctda.hcmus.edu.vn/vi/educational-program/"
    
    def __init__(self):
        super().__init__()

    async def scrape(self) -> AsyncIterator[tuple[str, bytes]]:
        await self.page.goto(self.BASE_URL, timeout=self.timeout)
        await self.page.wait_for_load_state("networkidle", timeout=self.network_idle)
        
        program_urls = await self.page.locator(".column_attr a").evaluate_all(
            """
            elements => elements
                .map(e => e.href)
                .filter(Boolean)
            """
        )
        
        program_urls = [url.replace("ctdb.hcmus.edu.vn", "ctda.hcmus.edu.vn") for url in program_urls]
        
        for url in program_urls:
            if 'viet-phap' in url:
                continue
            
            try:
                await self.page.goto(url, timeout=self.timeout)
                await self.page.wait_for_load_state("networkidle", timeout=self.network_idle)

                # --- 1. Text Parsing Section ---
                questions_locator = self.page.locator("div.question")
                count = await questions_locator.count()
                
                contents: List[str] = []
                for i in range(count):
                    question = questions_locator.nth(i)
                    title = (await question.locator("div.title").inner_text()).strip()
                    
                    answer = await question.locator("div.answer").evaluate("""
                        element => {
                            const clone = element.cloneNode(true);
                            clone.querySelectorAll('p[style], pre[style]').forEach(el => {
                                const style = (el.getAttribute('style') || '').toLowerCase();
                                if (style.includes('text-align: center')) {
                                    el.remove();
                                }
                            });
                            return clone.innerText.trim();
                        }
                    """)
                    contents.append(f"# {title}\n\n{answer}")

                if contents:
                    combined_content = "\n\n---\n\n".join(contents)
                    page_name = Path(url.rstrip("/")).name or self.SITE_NAME
                    yield f"{page_name}.txt", combined_content.encode("utf-8")
                    
                # --- 2. Attachment Parsing Section ---
                attachments_locator = self.page.locator(
                    "div.download-attachments li.pdf a.attachment-link"
                )

                file_urls = await attachments_locator.evaluate_all(
                    """
                    elements => elements
                        .map(e => ({
                            url: e.href,
                            name: e.textContent.trim()
                        }))
                        .filter(item => item.url)
                    """
                )

                if file_urls:
                    unique_files = {pdf["url"]: pdf for pdf in file_urls}.values()
                    
                    download_tasks = [
                        self._extract_attachment(pdf["url"], pdf["name"])
                        for pdf in unique_files
                    ]
                    
                    # Safe external network session concurrency
                    results = await asyncio.gather(*download_tasks)
                    
                    extracted_count = 0
                    for attachment in results:
                        if attachment is not None:
                            extracted_count += 1
                            yield attachment

                    if extracted_count > 0:
                        logger.info("Extracted %d attachments for: %s", extracted_count, url)

            except Exception:
                logger.exception("Failed to scrape page: %s", url)


    async def _extract_attachment(
        self,
        file_url: str,
        file_name: str | None = None,
    ) -> tuple[str, bytes] | None:
        if not self.session:
            raise RuntimeError("HTTP client is not initialized.")

        try:
            response = await self.session.get(file_url)
            response.raise_for_status()

            filename = self._attachment_filename(
                file_url=file_url,
                file_name=file_name,
                content_type=response.headers.get("Content-Type"),
            )

            content_bytes = response.content if isinstance(response.content, bytes) else await response.read()
            return filename, content_bytes

        except Exception as exc:
            logger.warning("Failed to extract attachment '%s': %s", file_url, exc)
            return None

    def _attachment_filename(
        self,
        *,
        file_url: str,
        file_name: str | None = None,
        content_type: str | None = None,
    ) -> str:
        filename = (file_name or Path(unquote(urlparse(file_url).path)).name).strip()
        extension = (
            Path(filename).suffix
            or mimetypes.guess_extension((content_type or "").split(";")[0])
            or ".bin"
        )
        return filename if Path(filename).suffix else f"{filename or 'attachment'}{extension}"
