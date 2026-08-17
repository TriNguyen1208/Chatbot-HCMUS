from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
from config.settings import settings

import json
import hashlib
from pathlib import Path

class Chunker:
    def __init__(self):
        headers_to_split_on = [
            ("#", "Header 1"),
            ("##", "Header 2"),
            ("###", "Header 3"),
        ]

        self.markdown_splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=headers_to_split_on,
            strip_headers=False  # Keep headers
        )

        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=500,    
            chunk_overlap=75,  
        )

        with open(settings.DATA_CACHE_DIR / "manifest.json", 'r', encoding='utf-8') as f:
            self.manifest_data = json.load(f)

    def _update_manifest_data(self):
        with open(settings.DATA_CACHE_DIR / "manifest.json", 'r', encoding='utf-8') as f:
            self.manifest_data = json.load(f)
    
    def _split_md_file(self, md_path: str) -> list[str]:
        with open(md_path, "r", encoding='utf-8') as f:
            md_text = f.read()

        if not md_text.strip():
            print("Error! File's empty!")
            return []
        
        # 1. markdown --> many (big) chunks (splitted by headers)
        md_header_splits = self.markdown_splitter.split_text(md_text)

        # 2. big chunks --> smaller chunks (if the chunk is too large)
        final_splits = self.text_splitter.split_documents(md_header_splits)

        chunks = [doc.page_content for doc in final_splits]
        return chunks

    def chunk_md_file(self, file_hash: str, force: bool = False, update_manifest_data: bool = True):
        """
            Chunking a markdown file (splitting it into chunks and save them in a jsonl file).

            Args:
                file_hash (str)
                force (bool): whether to force chunking the file even if it was already chunked before
                update_manifest_data (bool): wheter to update the manifest data before chunking
        """
        # 0. Get metadata from manifest.json
        if (update_manifest_data): self._update_manifest_data()

        if (file_hash not in self.manifest_data):
            raise Exception(f"Error: File hash '{file_hash}' not found in manifest.json!")
        
        manifest_item = self.manifest_data[file_hash]

        # 1. Decide whether to chunk
        processed_file_path = settings.DATA_PROCESSED_DIR / f"{manifest_item['site_name']}" / (file_hash + ".md")
        chunk_file_path = settings.DATA_CHUNK_DIR / f"{manifest_item['site_name']}" / (file_hash + ".jsonl")

        if (
            not force
            and chunk_file_path.exists()
            and chunk_file_path.stat().st_mtime >= processed_file_path.stat().st_mtime
        ):
            print(". Skip chunking file:", file_hash)
            return

        # 2. Split into chunks
        chunks_list = self._split_md_file(processed_file_path)

        # 3. Save chunks in a jsonl file
        document_metadata = {
            "document_id": file_hash,
            "source_type": manifest_item["site_name"],
            "original_filename": manifest_item["file_name"],
            "document_path": f"{manifest_item['site_name']}/{file_hash}.md",
            "document_title": Path(manifest_item["file_name"]).stem
        }

        chunk_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(chunk_file_path, 'w', encoding='utf-8') as f:
            for index, chunk_content in enumerate(chunks_list):
                chunk_metadata = document_metadata | {
                    "chunk_id": document_metadata["document_id"] + f"_{index:04d}",
                    "chunk_index": index,
                    "content": chunk_content,
                    "content_hash": hashlib.sha256(chunk_content.encode("utf-8")).hexdigest()[:16]
                }

                f.write(json.dumps(chunk_metadata, ensure_ascii=False) + "\n")
        
    def chunk_md_folder(self, folder: str, force: bool=False, update_manifest_data: bool=True):
        """
            Chunking a folder of markdown files. 

            Args:
                folder (str) = 'curriculum'/'information'/'announcement'/'all'
                force (bool): whether to force chunking the file even if it was already chunked before
                update_manifest_data (bool): wheter to update the manifest data before chunking
        """
        if (update_manifest_data):
            self._update_manifest_data()

        if (folder=='all'):
            site_folders = ['curriculum', 'information', 'announcement']
            for sf in site_folders:
                self.chunk_md_folder(sf, force=force, update_manifest_data=False)
            return

        folder_path = settings.DATA_PROCESSED_DIR / folder
        for file in folder_path.iterdir():
            if (file.is_file()):
                self.chunk_md_file(file.stem, force=force, update_manifest_data=False)

# Test ===
if __name__ == "__main__":
    chunker = Chunker()
    # -- chunk a single md file --
    # chunker.chunk_md_file('d21e986671e6fbec')
    # -- chunk a folder ('curriculum'/'information'/'announcement'/'all')
    chunker.chunk_md_folder('information')