import logging
from typing import List

from qdrant_client import QdrantClient
from qdrant_client.models import Fusion, FusionQuery, PointStruct, Prefetch, SparseVector

from ingestion.pipeline.embedder import Embedder

logger = logging.getLogger(__name__)


class QdrantSearch:
    """Handles semantic and hybrid retrieval from a Qdrant collection."""

    def __init__(self, qdrant_client: QdrantClient, embedder: Embedder) -> None:
        """
        Args:
            qdrant_client: An already-initialised QdrantClient instance.
            embedder: An Embedder instance used to produce query vectors.
        """
        self.client = qdrant_client
        self.embedder = embedder

    # ------------------------------------------------------------------
    # Semantic search  (dense only)
    # ------------------------------------------------------------------

    def semantic_search(
        self,
        collection_name: str,
        query_text: str,
        score_threshold: float = 0.7,
        limit: int = 5,
    ) -> List[PointStruct]:
        """
        Semantic search using the dense vector index.

        Args:
            collection_name: The Qdrant collection to query.
            query_text: The user query string.
            score_threshold: Minimum cosine similarity score for a result to be returned.
            limit: Maximum number of results to return.

        Returns:
            List of matching PointStruct objects.
        """
        logger.debug(
            "[Search] Semantic search: collection='%s', limit=%d, score_threshold=%s.",
            collection_name,
            limit,
            score_threshold,
        )
        query_vector = self.embedder.dense_query(query_text)
        points = self.client.query_points(
            collection_name=collection_name,
            query=query_vector,
            using="dense",
            score_threshold=score_threshold,
            limit=limit,
        ).points
        logger.debug(
            "[Search] Semantic search completed: collection='%s', results=%d.",
            collection_name,
            len(points),
        )
        return points

    # ------------------------------------------------------------------
    # Hybrid search  (dense + sparse → RRF)
    # ------------------------------------------------------------------

    def hybrid_search(
        self,
        collection_name: str,
        query_text: str,
        prefetch_limit: int = 20,
        limit: int = 10,
        dense_threshold: float = 0.45,
    ) -> List[PointStruct]:
        """
        Hybrid search combining dense and sparse (BM25) vectors with RRF fusion.

        Args:
            collection_name: The Qdrant collection to query.
            query_text: The user query string.
            prefetch_limit: Number of candidates to prefetch from each sub-index.
            limit: Maximum number of results to return after fusion.
            dense_threshold: Minimum dense cosine similarity score for dense prefetch candidates.

        Returns:
            List of matching PointStruct objects, ranked by RRF score.
        """
        logger.debug(
            "[Search] Hybrid search: collection='%s', limit=%d, prefetch_limit=%d, dense_threshold=%s.",
            collection_name,
            limit,
            prefetch_limit,
            dense_threshold,
        )
        dense_vector, sparse_embedding = self.embedder.hybrid_query(query_text)

        response = self.client.query_points(
            collection_name=collection_name,
            prefetch=[
                Prefetch(
                    query=dense_vector,
                    using="dense",
                    limit=prefetch_limit,
                    score_threshold=dense_threshold,
                ),
                Prefetch(
                    query=SparseVector(
                        indices=sparse_embedding.indices.tolist(),
                        values=sparse_embedding.values.tolist(),
                    ),
                    using="sparse",
                    limit=prefetch_limit,
                ),
            ],
            query=FusionQuery(fusion=Fusion.RRF),
            limit=limit,
        )
        points = response.points
        logger.debug(
            "[Search] Hybrid search completed: collection='%s', results=%d.",
            collection_name,
            len(points),
        )
        return points
