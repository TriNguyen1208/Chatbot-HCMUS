import asyncio
from pathlib import Path
import logging
from typing import List

from crawler_services.pipeline.scraping.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class CurriculumScraper(BaseScraper):
    SITE_NAME = "curriculum"
    BASE_URL = "https://www.ctda.hcmus.edu.vn/vi/educational-program/"
    
    def __init__(self, root_dir: Path):
        super().__init__(root_dir)
    
    async def scrape(self):
        self._make_site_directory()
        
        await self.page.goto(self.BASE_URL, timeout=self.timeout)
        await self.page.wait_for_load_state("networkidle", timeout=self.network_idle)
        
        program_urls = await self.page.locator(".column_attr a").evaluate_all(
            """
            elements => elements
                .map(e => e.href)
                .filter(Boolean)
            """
        )
        
        for url in program_urls:
            if 'viet-phap' in url:
                continue
            
            try:
                await self.page.goto(url, timeout=self.timeout)
                await self.page.wait_for_load_state("networkidle", timeout=self.network_idle)

                questions = self.page.locator("div.question")
                count = await questions.count()

                contents: List[str] = []
                
                for i in range(count):
                    question = questions.nth(i)

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
                    
                    contents.append(
                        f"# {title}\n\n{answer}"
                    )
                if contents:
                    combined_content = "\n\n---\n\n".join(contents)

                    page_name = (
                        Path(url).name
                        or self.SITE_NAME
                    )

                    await self._cache_document(
                        cache_key=url,
                        content=combined_content.encode("utf-8"),
                        extension=".txt",
                        filename=page_name,
                        content_type="text/plain",
                    )
                
                    
                attachments = self.page.locator(
                    "div.download-attachments li.pdf a.attachment-link"
                )

                file_urls = await attachments.evaluate_all(
                    """
                    elements => elements
                        .map(e => ({
                            url: e.href,
                            name: e.textContent.trim()
                        }))
                        .filter(item => item.url)
                    """
                )

                unique_files = {
                    pdf["url"]: pdf
                    for pdf in file_urls
                }.values()
                
                download_tasks = [
                    self._download_attachment(
                        pdf["url"],
                        pdf["name"],
                    )
                    for pdf in unique_files
                ]

                results = await asyncio.gather(*download_tasks)

                attachments = [
                    result
                    for result in results
                    if result is not None
                ]

                logger.info(
                    "Downloaded/reused %d attachments",
                    len(attachments),
                )

            except Exception:
                logger.exception(
                    "Failed to scrape page: %s",
                    url,
                )

async def main():
    async with CurriculumScraper(
        root_dir=Path("./data")
    ) as scraper:
        documents = await scraper.scrape()
        
if __name__ == "__main__":
    asyncio.run(main())