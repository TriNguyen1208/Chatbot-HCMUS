import logging
from pathlib import Path
from llama_cloud import AsyncLlamaCloud

from ingestion.config.settings import settings
from ingestion.pipeline.parser.base_parser import BaseParser

logger = logging.getLogger(__name__)


class LlamaParser(BaseParser):
    PARSER_NAME = "llama"

    def __init__(
        self,
        tier: str | None = None,
        api_key: str | None = None,
        client: AsyncLlamaCloud | None = None,
    ):
        super().__init__()

        self.tier = tier or settings.LLAMA_CLOUD_TIER
        self.max_workers = settings.LLAMA_CLOUD_MAX_WORKERS

        key = api_key or settings.LLAMA_CLOUD_API_KEY

        self.client = (
            client
            or AsyncLlamaCloud(api_key=key)
        )


    async def parse_file(
        self,
        input_path: Path,
        output_path: Path,
    ) -> None:

        if not input_path.exists():
            logger.error("Input file does not exist: %s", input_path)
            return

        try:
            file_obj = await self.client.files.create(
                file=str(input_path),
                purpose="parse",
            )

            result = await self.client.parsing.parse(
                file_id=file_obj.id,
                tier=self.tier,
                version="latest",
                expand=["markdown"],
            )

            if not result or not result.markdown:
                logger.error(
                    "Failed parsing file: %s",
                    input_path.name,
                )
                return

            markdown = "\\n\\n---\\n\\n".join(
                page.markdown
                for page in result.markdown.pages
                if page.markdown
            )

            output_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            output_path.write_text(
                markdown,
                encoding="utf-8",
            )

            logger.debug(
                "Parsed: %s -> %s",
                input_path.name,
                output_path.name,
            )

        except Exception as e:
            logger.exception(
                "Error parsing %s: %s",
                input_path.name,
                e,
            )
