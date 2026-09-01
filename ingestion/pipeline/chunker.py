from langchain_core.documents import Document
from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
from ingestion.config.settings import settings

import json
import hashlib
import logging
from pathlib import Path
from typing import List, Optional, Union
from tqdm import tqdm

logger = logging.getLogger(__name__)


class Chunker:
    def __init__(self, chunk_size: int = 500, chunk_overlap: int = 75):
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
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )

        self._update_manifest_data()

    def _update_manifest_data(self) -> None:
        """
        Load document metadata from manifest.json cache if it exists.
        """
        manifest_path = settings.DATA_CACHE_DIR / "manifest.json"
        if not manifest_path.exists():
            self.manifest_data = {}
            return

        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                raw_manifest = json.load(f)
                if isinstance(raw_manifest, list):
                    self.manifest_data = {
                        item["file_hash"]: item
                        for item in raw_manifest
                        if isinstance(item, dict) and "file_hash" in item
                    }
                elif isinstance(raw_manifest, dict):
                    self.manifest_data = raw_manifest
                else:
                    self.manifest_data = {}
        except Exception as e:
            logger.warning("Failed to load manifest.json: %s", e)
            self.manifest_data = {}

    def _split_md_file(self, md_path: Union[str, Path]) -> list[Document]:
        """
        Split a single Markdown file into Document chunks using header and recursive character splitters.
        """
        path = Path(md_path)
        if not path.exists():
            logger.error("File does not exist: %s", path)
            return []

        with open(path, "r", encoding="utf-8") as f:
            md_text = f.read()

        if not md_text.strip():
            logger.warning("File is empty: %s", path)
            return []

        # 1. markdown --> header-based chunks
        md_header_splits = self.markdown_splitter.split_text(md_text)

        # 2. big chunks --> smaller chunks
        final_splits = self.text_splitter.split_documents(md_header_splits)

        return final_splits

    def chunk_file(
        self,
        file_path: Union[str, Path],
        force: bool = False,
        update_manifest_data: bool = False,
    ) -> Optional[Path]:
        """
        Chunk a markdown file and save output as a JSONL file in DATA_CHUNK_DIR,
        preserving folder hierarchy relative to DATA_PROCESSED_DIR.

        Args:
            file_path: Path to the markdown file to chunk.
            force: Force re-chunking even if output already exists and is up to date.
            update_manifest_data: Whether to refresh manifest data before chunking.

        Returns:
            Optional[Path]: Path to generated jsonl chunk file, or None if skipped.
        """
        if update_manifest_data:
            self._update_manifest_data()

        path = Path(file_path).resolve()
        if not path.exists() or not path.is_file():
            logger.error("Markdown file not found: %s", path)
            return None

        file_hash = path.stem
        processed_dir = Path(settings.DATA_PROCESSED_DIR).resolve()

        # Determine relative subfolder path
        try:
            rel_path = path.relative_to(processed_dir)
            subfolder = rel_path.parent
        except ValueError:
            subfolder = Path(".")

        # Output chunk jsonl path
        chunk_file_path = (settings.DATA_CHUNK_DIR / subfolder / f"{file_hash}.jsonl").resolve()

        # Check freshness
        if (
            not force
            and chunk_file_path.exists()
            and chunk_file_path.stat().st_mtime >= path.stat().st_mtime
        ):
            logger.debug("Skip chunking file (already up to date): %s", path.name)
            return None

        # Split into chunks
        doc_splits = self._split_md_file(path)
        if not doc_splits:
            return None

        # Retrieve metadata from manifest or fallback
        manifest_item = self.manifest_data.get(file_hash, {})
        site_name = manifest_item.get("site_name") or (subfolder.as_posix() if subfolder != Path(".") else "general")
        file_name = manifest_item.get("file_name") or path.name

        document_metadata = {
            "document_id": file_hash,
            "source_type": site_name,
            "original_filename": file_name,
            "document_path": f"{site_name}/{path.name}" if site_name != "." else path.name,
            "document_title": Path(file_name).stem,
        }

        # Write chunks to JSONL
        chunk_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(chunk_file_path, "w", encoding="utf-8") as f:
            for index, doc in enumerate(doc_splits):
                chunk_metadata = document_metadata | {
                    "chunk_id": f"{document_metadata['document_id']}_{index:04d}",
                    "chunk_index": index,
                    "headers": doc.metadata,
                    "content": doc.page_content,
                    "content_hash": hashlib.sha256(doc.page_content.encode("utf-8")).hexdigest()[:16],
                }
                f.write(json.dumps(chunk_metadata, ensure_ascii=False) + "\n")

        logger.debug("Chunked: %s -> %s (%d chunks)", path.name, chunk_file_path.name, len(doc_splits))
        return chunk_file_path

    def chunk_folder(
        self,
        folder: Union[str, Path],
        force: bool = False,
        update_manifest_data: bool = False,
    ) -> List[Path]:
        """
        Chunk all markdown files in a specific folder.

        Args:
            folder: Subfolder name (under DATA_PROCESSED_DIR) or full Path to folder.
            force: Force re-chunking.
            update_manifest_data: Whether to refresh manifest data.

        Returns:
            List[Path]: List of generated/updated chunk JSONL paths.
        """
        if update_manifest_data:
            self._update_manifest_data()

        folder_path = Path(folder)
        if not folder_path.is_absolute():
            folder_path = Path(settings.DATA_PROCESSED_DIR) / folder

        if not folder_path.exists() or not folder_path.is_dir():
            logger.warning("Folder does not exist: %s", folder_path)
            return []

        md_files = [f for f in folder_path.iterdir() if f.is_file() and f.suffix == ".md"]
        chunked_files: List[Path] = []

        with tqdm(total=len(md_files), desc=f"Chunking {folder_path.name}", unit="file") as progress:
            for file in md_files:
                chunk_path = self.chunk_file(file, force=force, update_manifest_data=False)
                if chunk_path:
                    chunked_files.append(chunk_path)
                progress.update(1)

        return chunked_files

    def chunk_all(self, force: bool = False) -> List[Path]:
        """
        Directly chunk all markdown files found in DATA_PROCESSED_DIR across all subfolders.

        Args:
            force: Force re-chunking even if chunks are up to date.

        Returns:
            List[Path]: List of generated/updated chunk JSONL paths.
        """
        self._update_manifest_data()
        processed_dir = Path(settings.DATA_PROCESSED_DIR)

        if not processed_dir.exists():
            logger.warning("Processed directory does not exist: %s", processed_dir)
            return []

        chunked_files: List[Path] = []

        # 1. Process all subdirectories under DATA_PROCESSED_DIR
        for item in sorted(processed_dir.iterdir()):
            if item.is_dir():
                chunked_files.extend(self.chunk_folder(item, force=force, update_manifest_data=False))
            elif item.is_file() and item.suffix == ".md":
                chunk_path = self.chunk_file(item, force=force, update_manifest_data=False)
                if chunk_path:
                    chunked_files.append(chunk_path)

        logger.info("Total files chunked: %d across %s", len(chunked_files), processed_dir)
        return chunked_files

    # Backward-compatible API aliases
    def chunk_md_file(
        self,
        file_hash_or_path: Union[str, Path],
        force: bool = False,
        update_manifest_data: bool = True,
    ) -> Optional[Path]:
        """
        Legacy method for chunking a single markdown file by hash or Path.
        """
        if update_manifest_data:
            self._update_manifest_data()

        input_path = Path(file_hash_or_path)
        if input_path.exists() and input_path.is_file():
            return self.chunk_file(input_path, force=force, update_manifest_data=False)

        file_hash = str(file_hash_or_path)
        if file_hash in self.manifest_data:
            manifest_item = self.manifest_data[file_hash]
            processed_file_path = settings.DATA_PROCESSED_DIR / f"{manifest_item['site_name']}" / f"{file_hash}.md"
            if processed_file_path.exists():
                return self.chunk_file(processed_file_path, force=force, update_manifest_data=False)

        # Search for file in DATA_PROCESSED_DIR if not directly found
        for match in Path(settings.DATA_PROCESSED_DIR).rglob(f"{file_hash}.md"):
            return self.chunk_file(match, force=force, update_manifest_data=False)

        logger.error("File hash '%s' could not be resolved to a markdown file.", file_hash)
        return None

    def chunk_md_folder(
        self,
        folder: str = "all",
        force: bool = False,
        update_manifest_data: bool = True,
    ) -> List[Path]:
        """
        Legacy method for chunking a folder or all folders.
        """
        if update_manifest_data:
            self._update_manifest_data()

        if folder == "all":
            return self.chunk_all(force=force)

        return self.chunk_folder(folder, force=force, update_manifest_data=False)


# Test ===
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    chunker = Chunker()
    # Chunk all files directly from DATA_PROCESSED_DIR
    res = chunker.chunk_all(force=True)
    print(f"Successfully chunked {len(res)} files.")
