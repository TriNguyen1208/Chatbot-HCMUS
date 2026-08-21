from pathlib import Path
import hashlib
import logging
from typing import Any

from ingestion.config.settings import settings

logger = logging.getLogger(__name__)

class DocumentDownloader:
    def __init__(self):
        self.root_dir = settings.DATA_DIR
        self.root_dir.mkdir(parents=True, exist_ok=True)
        
        
    def _make_site_directory(self, site_name: str) -> Path:
        site_dir = settings.DATA_RAW_DIR / site_name
        site_dir.mkdir(parents=True, exist_ok=True)
        return site_dir


    def _storage_path(self, target_path: Path) -> str:
        return str(target_path.relative_to(self.root_dir))


    async def store_document(
        self,
        filename: str,
        file_bytes: bytes,
        site_name: str,
    ) -> dict[str, Any]:
        """
        Saves raw bytes using content hashing to completely prevent duplicate disk writes.
        """
        raw_dir = self._make_site_directory(site_name)
        extension = Path(filename).suffix or ".bin"
        
        file_hash = hashlib.sha256(file_bytes).hexdigest()[:16]
        target_path = raw_dir / f"{file_hash}{extension}"

        # Write to disk only if this exact file variant does not exist
        if not target_path.exists():
            target_path.write_bytes(file_bytes)
            logger.debug("Saved distinct document version: %s", target_path)
        else:
            logger.debug("Document version already exists on disk: %s", target_path)

        return {
            "file_name": filename,
            "file_path": self._storage_path(target_path),
            "file_size": len(file_bytes),
            "file_hash": file_hash,
        }
