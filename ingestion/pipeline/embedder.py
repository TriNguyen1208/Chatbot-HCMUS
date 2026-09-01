import logging
from typing import List, Tuple

import requests
from fastembed import SparseTextEmbedding, SparseEmbedding

logger = logging.getLogger(__name__)


class Embedder:
    """Handles all embedding operations: dense (TEI), sparse (fastembed), and hybrid."""

    def __init__(
        self,
        tei_url: str,
        sparse_model: str = "Qdrant/bm25",
    ) -> None:
        """
        Args:
            tei_url: Base URL of the local Text-Embedding-Inference server.
            sparse_model: fastembed sparse model name for BM25-style keyword embeddings.
        """
        self._tei_url = tei_url.rstrip("/") if tei_url else ""
        self._sparse_model = SparseTextEmbedding(model_name=sparse_model)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _post_tei(self, texts: List[str]) -> List[List[float]]:
        """Send a batch of texts to the TEI /embed endpoint and return dense vectors."""
        try:
            response = requests.post(
                f"{self._tei_url}/embed",
                json={"inputs": texts},
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            logger.error(
                "[Embedder] Failed to retrieve dense embeddings from TEI server at '%s': %s",
                self._tei_url,
                exc,
            )
            raise

    # ------------------------------------------------------------------
    # Public document-level API  (E5 "passage:" prefix)
    # ------------------------------------------------------------------

    def dense(self, texts: List[str]) -> List[List[float]]:
        """Return dense vectors for a list of document texts (passage: prefix)."""
        prefixed = [f"passage: {t}" for t in texts]
        logger.debug("[Embedder] Generating dense embeddings for %d document(s).", len(texts))
        return self._post_tei(prefixed)

    def sparse(self, texts: List[str]) -> List[SparseEmbedding]:
        """Return sparse BM25 embeddings for a list of document texts."""
        logger.debug("[Embedder] Generating sparse embeddings for %d document(s).", len(texts))
        return list(self._sparse_model.embed(texts))

    def hybrid(self, texts: List[str]) -> Tuple[List[List[float]], List[SparseEmbedding]]:
        """Return (dense_vectors, sparse_vectors) for a list of document texts."""
        dense_vectors = self.dense(texts)
        sparse_vectors = self.sparse(texts)
        return dense_vectors, sparse_vectors

    # ------------------------------------------------------------------
    # Public query-level API  (E5 "query:" prefix)
    # ------------------------------------------------------------------

    def dense_query(self, query: str) -> List[float]:
        """Return a single dense vector for a query string (query: prefix)."""
        logger.debug("[Embedder] Generating dense embedding for query.")
        return self._post_tei([f"query: {query}"])[0]

    def sparse_query(self, query: str) -> SparseEmbedding:
        """Return a single sparse embedding for a query string."""
        logger.debug("[Embedder] Generating sparse embedding for query.")
        return list(self._sparse_model.embed([query]))[0]

    def hybrid_query(self, query: str) -> Tuple[List[float], SparseEmbedding]:
        """Return (dense_vector, sparse_embedding) for a query string."""
        return self.dense_query(query), self.sparse_query(query)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def vector_dimension(self) -> int:
        """Probe the TEI server and return the dense vector dimension."""
        sample = self._post_tei(["test"])[0]
        dim = len(sample)
        logger.debug("[Embedder] Detected dense vector dimension: %d.", dim)
        return dim
