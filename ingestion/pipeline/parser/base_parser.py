from abc import ABC, abstractmethod
import asyncio
import logging
from pathlib import Path
from typing import Sequence
from tqdm.asyncio import tqdm_asyncio

from ingestion.config.settings import settings

logger = logging.getLogger(__name__)


class BaseParser(ABC):
    """
    Abstract base class for document ingestion parsers.
    """

    registry: dict[str, type["BaseParser"]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if hasattr(cls, "PARSER_NAME"):
            cls.registry[cls.PARSER_NAME] = cls


    def __init__(self):
        self.raw_dir = Path(settings.DATA_RAW_DIR)
        self.processed_dir = Path(settings.DATA_PROCESSED_DIR)


    @abstractmethod
    async def parse_file(self, input_path: Path, output_path: Path) -> None:
        pass


    async def parse_batch(
        self,
        file_pairs: Sequence[tuple[Path, Path]],
        max_workers: int = 5,
    ) -> None:

        semaphore = asyncio.Semaphore(max_workers)

        async def _worker(in_p: Path, out_p: Path):
            async with semaphore:
                await self.parse_file(in_p, out_p)

        tasks = [
            _worker(in_p, out_p)
            for in_p, out_p in file_pairs
        ]

        # Progress bar updates as tasks complete
        await tqdm_asyncio.gather(
            *tasks,
            desc="Parsing files",
            unit="file",
        )


    async def parse_directory(
        self,
        max_workers: int | None = None,
        force: bool = False,
    ) -> None:
        """
        Parse all files under settings.DATA_RAW_DIR into settings.DATA_PROCESSED_DIR.
        """

        r_dir = self.raw_dir
        p_dir = self.processed_dir

        if not r_dir.exists():
            logger.warning("Raw directory does not exist: %s", r_dir)
            return

        raw_files = [
            p for p in r_dir.rglob("*")
            if p.is_file() and not p.name.startswith(".")
        ]

        file_pairs: list[tuple[Path, Path]] = []

        for input_path in raw_files:
            rel_path = input_path.relative_to(r_dir)
            output_path = p_dir / rel_path.with_suffix(".md")

            should_parse = (
                force
                or not output_path.exists()
                or input_path.stat().st_mtime > output_path.stat().st_mtime
            )

            if should_parse:
                file_pairs.append((input_path, output_path))

        skipped = len(raw_files) - len(file_pairs)

        logger.info(
            "Found %d files | To parse: %d | Skipped: %d",
            len(raw_files),
            len(file_pairs),
            skipped,
        )

        if not file_pairs:
            logger.info("No files need parsing.")
            return

        workers = max_workers or getattr(self, "max_workers", 5)

        await self.parse_batch(
            file_pairs,
            max_workers=workers,
        )
