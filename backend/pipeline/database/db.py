import json
import uuid
import requests
from dotenv import load_dotenv
import os
from typing import List, Dict, Union, Any
from pathlib import Path
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, SparseVectorParams, SparseVector, \
    Prefetch, Fusion, FusionQuery, Filter, FieldCondition, MatchValue, Range
from fastembed import SparseTextEmbedding, SparseEmbedding

from config.settings import settings

load_dotenv()
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API = os.getenv("QDRANT_API")
TEI_URL = os.getenv("TEI_URL")
RERANKER_URL = os.getenv("RERANKER_URL")

class QdrantVectorDB:
    def __init__(self, qdrant_url: str = QDRANT_URL, api_key: str = QDRANT_API, tei_url: str = TEI_URL, sparse_embed_model: str = "Qdrant/bm25"):
        """
        Args:
            qdrant_url (str): URL of Qdrant's cluster.
            api_key (str): Qdrant's API key.
            tei_url (str): URL of local Text-Embedding-Inference server
            sparse_embed_model (str): Name of the model embedding texts into sparse vectors (for keyword search).
        """
        self.client = QdrantClient(url=qdrant_url, api_key=api_key)
        self.tei_url = tei_url.rstrip("/")
        self.sparse_embed_model = SparseTextEmbedding(model_name=sparse_embed_model)
        self.vector_size = self._get_vector_dimension()

    def _get_dense_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        response = requests.post(
            f"{self.tei_url}/embed",
            json={"inputs": texts},
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        return response.json()

    def _get_sparse_embeddings_batch(self, texts: List[str]) -> List[SparseEmbedding]:
        return list(self.sparse_embed_model.embed(texts))

    def _get_vector_dimension(self) -> int:
        sample_vector = self._get_dense_embeddings_batch(["test"])[0]
        return len(sample_vector)

    def init_collection(self, collection_name: str):
        """
        Initialize (create) a collection in the database (if not existed).

        Args:
            collection_name (str): the collection's name.
        """
        if not self.client.collection_exists(collection_name=collection_name):
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config= {   
                    "dense": VectorParams(size=self.vector_size, distance=Distance.COSINE)
                },
                sparse_vectors_config={
                    "sparse": SparseVectorParams()
                }
            )
            print(f"-> Initialized collection '{collection_name}' (Vector Dim: {self.vector_size})")

    def upload_jsonl(self, collection_name: str, file_path: Union[str, Path], batch_size: int = 16):
        """
        Upload chunks in a jsonl file to a Qdrant collection.

        Args:
            collection_name (str): the collection's name.
            file_path (str/Patb): the jsonl file path (standing at backend/).
            batch_size (int): size of chunks batch to be embedded by TEI server.
        """
        batch_texts = []
        batch_payloads = []
        batch_ids = []
        total_uploaded = 0

        with open(file_path, "r", encoding="utf-8") as f:
            for idx, line in enumerate(f):
                line_str = line.strip()
                if not line_str:
                    continue
                
                data = json.loads(line_str)
                content_text = data.get("content")
                
                if not content_text:
                    continue

                # 1. chunk_id --> UUID
                raw_chunk_id = data.get("chunk_id", str(idx))
                point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, raw_chunk_id))

                # 2. chunk's data --> payload
                payload = data.copy()

                batch_texts.append(content_text)
                batch_ids.append(point_id)
                batch_payloads.append(payload)

                # 3. embed chunk's content
                if len(batch_texts) >= batch_size:
                    self._process_and_upsert(collection_name, batch_ids, batch_texts, batch_payloads)
                    total_uploaded += len(batch_texts)
                    print(f"Uploaded {total_uploaded} chunks...")
                    batch_texts, batch_payloads, batch_ids = [], [], []

            # Uploaded remaining chunks
            if batch_texts:
                self._process_and_upsert(collection_name, batch_ids, batch_texts, batch_payloads)
                total_uploaded += len(batch_texts)
                print(f"Successfully uploaded {total_uploaded} chunks!")

    def upload_jsonl_folder(self, collection_name: str, folder: str, batch_size: int = 16):
        """
        Read all jsonl files in a folder, and upsert all their chunks to Qdrant collection.

        Args:
            collection_name (str): the collection's name.
            folder (str) = 'curriculum'/'information'/'announcement'/'all'
            batch_size (int): size of chunks batch to be embedded by TEI server.
        """
        if (folder=='all'):
            site_folders = ['curriculum', 'information', 'announcement']
            for sf in site_folders:
                self.upload_jsonl_folder(collection_name, sf, batch_size)
            return

        folder_path = settings.DATA_CHUNK_DIR / folder
        for file_path in folder_path.glob("*.jsonl"):
            if file_path.is_file():
                self.upload_jsonl(collection_name, file_path, batch_size)

    def _process_and_upsert(self, collection_name: str, ids: List[str], texts: List[str], payloads: List[Dict]):
            e5_texts = [f"passage: {t}" for t in texts]
            dense_vectors = self._get_dense_embeddings_batch(e5_texts)
            sparse_vectors = self._get_sparse_embeddings_batch(texts)

            points = [
                PointStruct(
                    id=p_id, 
                    vector= {
                        "dense": d_vec,
                        "sparse": SparseVector(indices=s_vec.indices, values=s_vec.values)
                    },
                    payload=p_load
                ) 
                for p_id, d_vec, s_vec, p_load in zip(ids, dense_vectors, sparse_vectors, payloads)
            ]
            self.client.upsert(collection_name=collection_name, points=points)

    def semantic_search(self, collection_name: str, query_text: str, score_threshold: float = 0.7, limit: int = 5) -> List[PointStruct]:
        """
        Semantic search.

        Args:
            collection_name (str)
            query_text (str)
            score_threshold (float): A point is matched if its matching score exceeds this threshold.
            limit (int): maximum number of returned points

        Return:
            List[PointStruct]
        """
        query_vector = self._get_dense_embeddings_batch([f"query : {query_text}"])[0]
        return self.client.query_points(
            collection_name=collection_name,
            query=query_vector,
            using="dense",
            score_threshold=score_threshold,
            limit=limit
        ).points

    def hybrid_search(self, collection_name: str, query_text: str, prefetch_limit=20, fusion_limit: int = 10, dense_threshold: float = 0.45) -> List[PointStruct]:
            """
            Hybrid search combining dense (semantic) and sparse (keyword) retrievals using Reciprocal Rank Fusion (RRF).

            Args:
                collection_name (str): The name of the Qdrant collection to search in.
                query_text (str): The raw search query provided by the user.
                prefetch_limit (int): Max number of points to retrieve in the Prefetch phase for each modality (Dense/Sparse)
                fusion_limit (int): Max number of points to return after fusing the results via RRF.
                dense_threshold (float): A point is semantically matched if its matching score exceeds this threshold.

            Returns:
                List[PointStruct]
            """
            dense_query = self._get_dense_embeddings_batch([f"query: {query_text}"])[0]
            sparse_query = self._get_sparse_embeddings_batch([query_text])[0]

            response = self.client.query_points(
                collection_name=collection_name,
                prefetch=[
                    Prefetch(
                        query=dense_query,
                        using="dense",
                        limit=prefetch_limit,
                        score_threshold=dense_threshold
                    ),
                    Prefetch(
                        query=SparseVector(
                            indices=sparse_query.indices.tolist(),
                            values=sparse_query.values.tolist()
                        ),
                        using="sparse",
                        limit=prefetch_limit
                    ),
                ],
                # Union of Dense and Sparse results, filter with RRF score
                query=FusionQuery(fusion=Fusion.RRF),
                limit=fusion_limit 
            )
            
            return response.points

    def _rerank_points(
        self, 
        query_text: str, 
        points: List[PointStruct], 
        reranker_url: str = RERANKER_URL
    ) -> List[PointStruct]:
        if not points:
            return []

        texts = [p.payload.get("content", "") for p in points]
        payload = {
            "query": query_text,
            "texts": texts,
            "truncate": True
        }
        
        response = requests.post(reranker_url, json=payload)
        response.raise_for_status()
        results = response.json()  # [{"index": 0, "score": 0.92}, {"index": 2, "score": 0.15}, ...]

        reranked_points = []
        for item in results:
            idx = item["index"]
            score = item["score"]
            
            point = points[idx]
            point.score = score  
            reranked_points.append(point)

        return reranked_points

    def hybrid_search_with_rerank(
        self, 
        collection_name: str, 
        query_text: str, 
        prefetch_limit=20, 
        fusion_limit: int = 10, 
        dense_threshold: float = 0.45,
        top_k: int = 5, 
        rerank_threshold: float = 0.3,
        reranker_url: str = RERANKER_URL
    ) -> List[PointStruct]:
        """
        Hybrid search, with Reranking (Cross-Encoder) at the end to filter and re-rank chunks.

        Args:
            collection_name (str): The name of the Qdrant collection to search in.
            query_text (str): The raw search query provided by the user.
            prefetch_limit (int): Max number of points to retrieve in the Prefetch phase for each modality (Dense/Sparse)
            fusion_limit (int): Max number of points to return after fusing the results via RRF.
            dense_threshold (float): A point is semantically matched if its matching score exceeds this threshold.
            top_k (int): The final max number of points to return.
            rerank_threshold (float): After reranking, only chunks with score exceeding this threshold are chosen.
            reranker_url (str): The HTTP endpoint URL for the TEI Reranker container. 

        Returns:
            List[PointStruct]
        """
        candidate_points = self.hybrid_search(
            collection_name=collection_name, 
            query_text=query_text, 
            prefetch_limit=prefetch_limit,
            fusion_limit=fusion_limit, 
            dense_threshold=dense_threshold,
        )

        if not candidate_points:
            return []

        reranked_points = self._rerank_points(
            query_text=query_text, 
            points=candidate_points, 
            reranker_url=reranker_url
        )

        filtered_points = [p for p in reranked_points if p.score >= rerank_threshold]

        return filtered_points[:top_k]

    @staticmethod
    def _build_block_specs(hits: List[Dict[str, Any]], extra_chunks: int = 2) -> List[Dict[str, Any]]:
        """
        Build blocks' specification for chunks.

        Args:
            hits: list of chunks.
            extra_chunks: number of extended chunks at 2 heads of each input chunk.
        
        Return:
            list of blocks' specification
        """
        if not hits:
            return []

        grouped: Dict[str, List[Dict]] = {}
        for item in hits:
            grouped.setdefault(item["document_id"], []).append(item)

        block_specs = []

        for doc_id, items in grouped.items():
            items.sort(key=lambda x: x["chunk_index"])

            # Expand [index - extra, index + extra]
            ranges = []
            for item in items:
                idx = item["chunk_index"]
                ranges.append({
                    "start": max(0, idx - extra_chunks),
                    "end": idx + extra_chunks,
                    "sample": item
                })

            # Merge overlapping blocks
            merged_ranges = []
            for r in ranges:
                if not merged_ranges:
                    merged_ranges.append(r)
                else:
                    prev = merged_ranges[-1]
                    if r["start"] <= prev["end"] + 1:
                        prev["end"] = max(prev["end"], r["end"])
                    else:
                        merged_ranges.append(r)

            # 
            for m in merged_ranges:
                block_specs.append({
                    "document_id": doc_id,
                    "start_idx": m["start"],
                    "end_idx": m["end"],
                    "sample": m["sample"]
                })

        return block_specs

    def _fetch_block_content(self, collection_name: str, spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a complete context block based on its specification.
        """
        scroll_filter = Filter(
            must=[
                FieldCondition(key="document_id", match=MatchValue(value=spec["document_id"])),
                FieldCondition(key="chunk_index", range=Range(gte=spec["start_idx"], lte=spec["end_idx"]))
            ]
        )

        records, _ = self.client.scroll(
            collection_name=collection_name,
            scroll_filter=scroll_filter,
            limit=100,
            with_payload=True,
            with_vectors=False
        )

        chunks = [r.payload for r in records if r.payload]
        chunks.sort(key=lambda x: x["chunk_index"])

        combined_text = "\n\n".join([c["content"] for c in chunks])

        block = spec["sample"].copy()
        block["content"] = combined_text
        block["start_chunk_index"] = chunks[0]["chunk_index"] if chunks else spec["start_idx"]
        block["end_chunk_index"] = chunks[-1]["chunk_index"] if chunks else spec["end_idx"]
        return block

    def build_context_blocks(
        self, 
        collection_name: str, 
        raw_hits: List[Dict[Any, Any]],
        extra_chunks: int = 2, 
    ) -> List[Dict[str, Any]]:
        """
        Build blocks of context from input chunks (raw_hits) by extending chunks and merging consecutive chunks after extending.
        """
        if not raw_hits:
            return []

        block_specs = self._build_block_specs(raw_hits, extra_chunks=extra_chunks)
        blocks = [self._fetch_block_content(collection_name, spec) for spec in block_specs]

        return blocks

# Testing ==========================
if __name__ == "__main__":
    db = QdrantVectorDB()
    COLLECTION_NAME = "HCMUS-DATA"

    # 1. Init Collection
    db.init_collection(collection_name=COLLECTION_NAME)

    # 2. Upload JSONLs
    db.upload_jsonl_folder(collection_name=COLLECTION_NAME, folder="all", batch_size=32)

    # 3. Search
    # results = db.semantic_search(
    #     collection_name=COLLECTION_NAME,
    #     query_text="Đăng ký đồ án tốt nghiệp 2022",
    #     score_threshold=0.5,
    #     limit=3
    # )

    # results = db.search_with_rerank(COLLECTION_NAME, "Đăng ký đồ án tốt nghiệp 2022")

    # for res in results:
    #     print(f"Score: {res.score:.4f} | Content: {res.payload.get('content')}")
    # print("Done!")