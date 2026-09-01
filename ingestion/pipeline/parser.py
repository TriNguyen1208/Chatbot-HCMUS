from llama_cloud import AsyncLlamaCloud
import asyncio
import logging
from pathlib import Path
from tqdm import tqdm

from ingestion.config.settings import settings


logger = logging.getLogger(__name__)


class Parser:
    def __init__(self, max_concurrency: int = settings.LLAMA_CLOUD_CONCURRENCY):
        self.client = AsyncLlamaCloud()
        self.max_concurrency = max_concurrency


    async def parse_file(
        self,
        input_path: Path,
        output_path: Path,
        tier: str = settings.LLAMA_CLOUD_TIER,
    ) -> None: 

        """
        Parse a single file with LlamaCloud and save the result as Markdown.
        """

        logger.debug("Parsing file: %s", input_path)

        # 1. Upload file to LlamaCloud
        file = await self.client.files.create(file=str(input_path), purpose="parse")

        # 2. Parse file
        result = await self.client.parsing.parse(
            file_id=file.id,
            tier=tier,
            version="latest",
            expand=["markdown"],
        )

        if not result or result.markdown is None:
            raise RuntimeError(f"LlamaCloud returned no markdown for: {input_path}")

        # Combine page-level Markdown
        full_markdown = "\n\n---\n\n".join(page.markdown for page in result.markdown.pages)

        # 3. Save to file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(full_markdown, encoding="utf-8")
        logger.debug("Parsed file: %s", input_path)


    async def parse_files(
        self,
        input_folder: Path,
        output_folder: Path,
        tier: str = settings.LLAMA_CLOUD_TIER,
    ) -> None:
        
        """
        Parse multiple files concurrently.
        """

        if not input_folder.exists():
            logger.warning("Input folder does not exist: %s", input_folder)
            return

        input_files = [f for f in input_folder.iterdir() if f.is_file()]

        if not input_files:
            logger.info("No files to parse in: %s", input_folder)
            return

        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def parse_one(input_path: Path) -> None: 
            
            output_path = (output_folder / f"{input_path.stem}.md") 
            
            async with semaphore:
                try:
                    await self.parse_file(
                        input_path=input_path,
                        output_path=output_path,
                        tier=tier,
                    )
                except Exception:
                    logger.exception("Failed to parse: %s", input_path,)

        tasks = [asyncio.create_task(parse_one(path)) for path in input_files]

        with tqdm(total=len(tasks), desc=f"Parsing {input_folder.name}", unit="file") as progress: 
            for task in asyncio.as_completed(tasks): 
                await task 
                progress.update(1)


    async def parse_all(self, tier: str = settings.LLAMA_CLOUD_TIER) -> None:
        
        """
        Parse all folders under DATA_RAW_DIR and preserve the folder structure under DATA_PROCESSED_DIR.
        """

        raw_dir = Path(settings.DATA_RAW_DIR)
        processed_dir = Path(settings.DATA_PROCESSED_DIR)

        for input_folder in raw_dir.iterdir():
            if not input_folder.is_dir():
                continue

            output_folder = (processed_dir / input_folder.name)

            await self.parse_files(
                input_folder=input_folder,
                output_folder=output_folder,
                tier=tier,
            )