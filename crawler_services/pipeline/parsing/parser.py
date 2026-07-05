from llama_cloud import LlamaCloud
from dotenv import load_dotenv
import time

load_dotenv()
client = LlamaCloud()

class FileParser:
    @staticmethod
    def toMd(input_path: str, output_path: str, tier: str = "cost_effective"):
        """
            Parse a file with LLama-Parser and save the result as Markdown.

            Args:
                input_path: path to pdf/images/docs/excel/ppt/... files
                output_path: path to save the markdown file
                tier: model's tier option ("fast" < "cost_effective" < "agentic" < "agentic_plus")

            Returns:
                None
        """
        file = client.files.create(file=input_path, purpose='parse')
        result = client.parsing.parse(
            file_id=file.id,
            tier=tier,
            version="latest",
            expand=["markdown"],
        )

        full_markdown = "\n\n---\n\n".join([page.markdown for page in result.markdown.pages])
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(full_markdown)

# Test code ----------
if __name__ == "__main__":
    start = time.time()
    FileParser.toMd('test_files/tkb.pdf', 'test_files/tkb.md')
    print("Running time:", time.time() - start)