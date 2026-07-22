import asyncio
import logging

from logger_config import setup_logging
from pipeline.parser.llama_parser import LlamaParser

setup_logging(level=logging.INFO)

logger = logging.getLogger(__name__)


async def main():
    logger.info("Starting ingestion engine...")

    parser = LlamaParser()
    await parser.parse_directory()

    logger.info("Ingestion engine run completed.")


if __name__ == "__main__":
    asyncio.run(main())