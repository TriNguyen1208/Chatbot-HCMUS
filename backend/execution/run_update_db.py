from pipeline.database.db import QdrantVectorDB
COLLECTION_NAME = "HCMUS-DATA"

if __name__ == "__main__":
    db = QdrantVectorDB()
    db.init_collection(collection_name=COLLECTION_NAME)
    db.upload_jsonl_folder(collection_name=COLLECTION_NAME, folder="all", batch_size=32)