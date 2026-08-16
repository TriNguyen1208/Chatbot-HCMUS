import json
import uuid
import requests
from dotenv import load_dotenv
import os
from typing import List, Dict, Union
from pathlib import Path
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

from config.settings import settings

load_dotenv()
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API = os.getenv("QDRANT_API")
TEI_URL = os.getenv("TEI_URL")

class QdrantVectorDB:
    def __init__(self, qdrant_url: str = QDRANT_URL, api_key: str = QDRANT_API, tei_url: str = TEI_URL):
        """
        Args:
            qdrant_url (str): URL of Qdrant's cluster.
            api_key (str): Qdrant's API key.
            tei_url (str): URL of local Text-Embedding-Inference server
        """
        self.client = QdrantClient(url=qdrant_url, api_key=api_key)
        self.tei_url = tei_url.rstrip("/")
        self.vector_size = self._get_vector_dimension()

    def _get_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        response = requests.post(
            f"{self.tei_url}/embed",
            json={"inputs": texts},
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        return response.json()

    def _get_vector_dimension(self) -> int:
        sample_vector = self._get_embeddings_batch(["test"])[0]
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
                vectors_config=VectorParams(size=self.vector_size, distance=Distance.COSINE)
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
            vectors = self._get_embeddings_batch(texts)
            points = [
                PointStruct(id=p_id, vector=vec, payload=p_load)
                for p_id, vec, p_load in zip(ids, vectors, payloads)
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
        query_vector = self._get_embeddings_batch([query_text])[0]
        return self.client.query_points(
            collection_name=collection_name,
            query=query_vector,
            score_threshold=score_threshold,
            limit=limit
        ).points

# Testing ==========================
if __name__ == "__main__":
    db = QdrantVectorDB()
    COLLECTION_NAME = "MyCollection"

    # 1. Init Collection
    db.init_collection(collection_name=COLLECTION_NAME)

    # 2. Upload JSONLs
    # db.upload_jsonl_folder(collection_name=COLLECTION_NAME, folder="all", batch_size=16)

    # 3. Search
    results = db.semantic_search(
        collection_name=COLLECTION_NAME,
        query_text="Đăng ký đồ án tốt nghiệp 2022",
        score_threshold=0.5,
        limit=3
    )

    for res in results:
        print(f"Score: {res.score:.4f} | Content: {res.payload.get('content')}")
    print("Done!")