import json
import logging
import uuid
from pathlib import Path
from typing import Dict, List, Union

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    PointStruct,
    SparseVector,
    SparseVectorParams,
    VectorParams,
)

from ingestion.config.settings import settings
from ingestion.pipeline.embedder import Embedder

logger = logging.getLogger(__name__)


class QdrantVectorDB:
    """Manages Qdrant collections and handles JSONL ingestion with hybrid embeddings."""

    def __init__(
        self,
        qdrant_url: str = None,
        api_key: str = None,
        embedder: Embedder = None,
    ) -> None:
        """
        Args:
            qdrant_url: URL of the Qdrant cluster. Defaults to settings.QDRANT_URL.
            api_key: Qdrant API key. Defaults to settings.QDRANT_API.
            embedder: An Embedder instance. If None, one is created from settings.TEI_URL.
        """
        _url = qdrant_url or settings.QDRANT_URL
        _key = api_key or settings.QDRANT_API

        self.client = QdrantClient(url=_url, api_key=_key)

        if embedder is None:
            embedder = Embedder(tei_url=settings.TEI_URL)
        self.embedder = embedder

        self.vector_size = self.embedder.vector_dimension()

    # ------------------------------------------------------------------
    # Collection management
    # ------------------------------------------------------------------

    def init_collection(self, collection_name: str) -> None:
        """
        Initialize (create) a Qdrant collection if it does not already exist.

        Args:
            collection_name: The collection name.
        """
        if not self.client.collection_exists(collection_name=collection_name):
            logger.info(
                "[Qdrant] Creating collection '%s' (dense_dim=%d).",
                collection_name,
                self.vector_size,
            )
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config={
                    "dense": VectorParams(size=self.vector_size, distance=Distance.COSINE)
                },
                sparse_vectors_config={
                    "sparse": SparseVectorParams()
                },
            )
        else:
            logger.debug("[Qdrant] Collection '%s' already exists.", collection_name)

    # ------------------------------------------------------------------
    # JSONL ingestion
    # ------------------------------------------------------------------

    def upload_jsonl(
        self,
        collection_name: str,
        file_path: Union[str, Path],
        batch_size: int = 16,
    ) -> None:
        """
        Upload chunks from a JSONL file into a Qdrant collection.

        Args:
            collection_name: The target collection name.
            file_path: Path to the JSONL file.
            batch_size: Number of chunks to embed and upsert per batch.
        """
        path_obj = Path(file_path)
        if not path_obj.exists():
            logger.error("[Qdrant] File not found: file='%s'.", path_obj.name)
            return

        logger.info(
            "[Qdrant] Starting JSONL upload: file='%s', collection='%s', batch_size=%d.",
            path_obj.name,
            collection_name,
            batch_size,
        )

        batch_texts: List[str] = []
        batch_payloads: List[Dict] = []
        batch_ids: List[str] = []
        total_uploaded = 0

        with open(path_obj, "r", encoding="utf-8") as fh:
            for idx, line in enumerate(fh):
                line_str = line.strip()
                if not line_str:
                    continue

                data = json.loads(line_str)
                content_text = data.get("content")

                if not content_text:
                    logger.warning(
                        "[Qdrant] Skipping record without content: file='%s', line=%d.",
                        path_obj.name,
                        idx + 1,
                    )
                    continue

                # chunk_id → deterministic UUID
                raw_chunk_id = data.get("chunk_id", str(idx))
                point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, raw_chunk_id))

                batch_texts.append(content_text)
                batch_ids.append(point_id)
                batch_payloads.append(data.copy())

                if len(batch_texts) >= batch_size:
                    self._process_and_upsert(collection_name, batch_ids, batch_texts, batch_payloads)
                    total_uploaded += len(batch_texts)
                    logger.debug(
                        "[Qdrant] Uploaded batch: file='%s', batch_size=%d, total_uploaded=%d.",
                        path_obj.name,
                        len(batch_texts),
                        total_uploaded,
                    )
                    batch_texts, batch_payloads, batch_ids = [], [], []

        # Flush remaining records
        if batch_texts:
            batch_count = len(batch_texts)
            self._process_and_upsert(collection_name, batch_ids, batch_texts, batch_payloads)
            total_uploaded += batch_count
            logger.debug(
                "[Qdrant] Uploaded batch: file='%s', batch_size=%d, total_uploaded=%d.",
                path_obj.name,
                batch_count,
                total_uploaded,
            )

        if total_uploaded == 0:
            logger.warning(
                "[Qdrant] Empty JSONL file or no valid records uploaded: file='%s'.",
                path_obj.name,
            )

        logger.info(
            "[Qdrant] JSONL upload completed: file='%s', total_chunks=%d, collection='%s'.",
            path_obj.name,
            total_uploaded,
            collection_name,
        )

    def upload_jsonl_folder(
        self,
        collection_name: str,
        folder: str,
        batch_size: int = 16,
    ) -> None:
        """
        Upload all JSONL files in a named subfolder of DATA_CHUNK_DIR into a Qdrant collection.

        Args:
            collection_name: The target collection name.
            folder: Subfolder name – one of 'curriculum', 'information', 'announcement', or 'all'.
            batch_size: Number of chunks to embed and upsert per batch.
        """
        if folder == "all":
            site_folders = ["curriculum", "information", "announcement"]
            logger.info(
                "[Qdrant] Starting upload for all folders to collection='%s'.",
                collection_name,
            )
            for sf in site_folders:
                self.upload_jsonl_folder(collection_name, sf, batch_size)
            logger.info(
                "[Qdrant] Completed upload for all folders to collection='%s'.",
                collection_name,
            )
            return

        folder_path = settings.DATA_CHUNK_DIR / folder
        if not folder_path.exists():
            logger.warning(
                "[Qdrant] Folder does not exist: folder='%s', path='%s'.",
                folder,
                folder_path,
            )
            return

        files = [f for f in folder_path.glob("*.jsonl") if f.is_file()]
        logger.info(
            "[Qdrant] Starting folder upload: folder='%s', files=%d, collection='%s'.",
            folder,
            len(files),
            collection_name,
        )

        for file_path in files:
            self.upload_jsonl(collection_name, file_path, batch_size)

        logger.info(
            "[Qdrant] Folder upload completed: folder='%s', collection='%s'.",
            folder,
            collection_name,
        )

    # ------------------------------------------------------------------
    # Internal: embed + upsert
    # ------------------------------------------------------------------

    def _process_and_upsert(
        self,
        collection_name: str,
        ids: List[str],
        texts: List[str],
        payloads: List[Dict],
    ) -> None:
        dense_vectors, sparse_vectors = self.embedder.hybrid(texts)

        points = [
            PointStruct(
                id=p_id,
                vector={
                    "dense": d_vec,
                    "sparse": SparseVector(
                        indices=s_vec.indices.tolist(),
                        values=s_vec.values.tolist(),
                    ),
                },
                payload=p_load,
            )
            for p_id, d_vec, s_vec, p_load in zip(ids, dense_vectors, sparse_vectors, payloads)
        ]
        self.client.upsert(collection_name=collection_name, points=points)


# ------------------------------------------------------------------
# Script entry-point (for ad-hoc testing)
# ------------------------------------------------------------------
if __name__ == "__main__":
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    _logger = _logging.getLogger(__name__)

    _embedder = Embedder(tei_url=settings.TEI_URL)
    _db = QdrantVectorDB(embedder=_embedder)
    COLLECTION_NAME = "TempCollection"

    _db.init_collection(collection_name=COLLECTION_NAME)
    _db.upload_jsonl_folder(collection_name=COLLECTION_NAME, folder="all", batch_size=32)
    _logger.info("[Test] Done!")
