from llama_cloud import LlamaCloud, AsyncLlamaCloud
from dotenv import load_dotenv
import time

import asyncio

load_dotenv()
client = LlamaCloud()

class FileParser:
    MAX_WORKERS = 5

    @staticmethod
    async def _parse_single_file_async(client: AsyncLlamaCloud, input_path: str, output_path: str, tier: str):
        """
        Parse a file with LLama-Parser and save the result as Markdown.

        Args:
            client
            input_path: path to pdf/images/docs/excel/ppt/... files
            output_path: path to save the markdown file
            tier: model's tier option ("fast" < "cost_effective" < "agentic" < "agentic_plus")

        Returns:
            None
        """
        file = await client.files.create(file=input_path, purpose='parse')
        result = await client.parsing.parse(
            file_id=file.id,
            tier=tier,
            version="latest",
            expand=["markdown"],
        )

        if not result or result.markdown is None:
            print("Fail when parsing file:", input_path)
            return

        full_markdown = "\n\n---\n\n".join([page.markdown for page in result.markdown.pages])
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(full_markdown)

    @staticmethod
    def toMd(input_path: str, output_path: str, tier: str = "cost_effective"):
        """
        Parse a single file with LlamaParse and save the result as markdown

        Args:
            input_path: path to the input file
            output_path: path to save the markdown result file
            tier: model's tier option ("fast" < "cost_effective" < "agentic" < "agentic_plus")

        Returns:
            None
        """
        async def single_task():
            client = AsyncLlamaCloud()
            await FileParser._parse_single_file_async(client, input_path, output_path, tier)

        asyncio.run(single_task())

    @staticmethod
    async def _parse_with_semaphore(client: AsyncLlamaCloud, input_path: str, output_path: str, tier: str, semaphore: asyncio.Semaphore):
        async with semaphore:
            await FileParser._parse_single_file_async(client, input_path, output_path, tier)

    @staticmethod
    def toMdBatch(file_pairs: list[tuple[str, str]], tier: str = "cost_effective"):
        """
        Parse multiple files parallelly with LLama-Parser and save the results as Markdown.

        Args:
            file_pairs: list of (input_path, output_path)
            tier: model's tier option ("fast" < "cost_effective" < "agentic" < "agentic_plus")

        Returns:
            None
        """
        async def parallel_tasks():
            client = AsyncLlamaCloud()
            semaphore = asyncio.Semaphore(FileParser.MAX_WORKERS)
            tasks = [
                FileParser._parse_with_semaphore(client, in_path, out_path, tier, semaphore)
                for in_path, out_path in file_pairs
            ]
            await asyncio.gather(*tasks)
        asyncio.run(parallel_tasks())

# Test code ----------
if __name__ == "__main__":
    start = time.time()

    # FileParser.toMd('test_files/congvan.jpg', 'test_files/congvan.md')
    FileParser.toMdBatch(
        [
            ('test_files/congvan.jpg', 'test_files/congvan.md'),
            ('test_files/data_mining.pdf', 'test_files/data_mining.md'),
            ('test_files/excel.xlsx', 'test_files/excel.md')
        ]
    )
    print("Running time:", time.time() - start)