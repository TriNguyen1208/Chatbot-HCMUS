from pipeline.chunker import Chunker
from pipeline.parser.parser import FileParser

if __name__ == "__main__":
    # 1. Parse files
    print("Phase 1: Parsing...")
    FileParser.parse_folder('all')

    # 2. Chunk md files
    print("Phase 2: Chunking...")
    chunker = Chunker()
    chunker.chunk_md_folder('all')