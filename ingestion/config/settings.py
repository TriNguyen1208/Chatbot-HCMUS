from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from typing import Literal

class Settings(BaseSettings):
    # Paths
    BASE_DIR: Path = Path(__file__).resolve().parent.parent
    DATA_DIR: Path = BASE_DIR / "data"
    DATA_RAW_DIR: Path = DATA_DIR / "raw"
    DATA_PROCESSED_DIR: Path = DATA_DIR / "processed"
    DATA_CACHE_DIR: Path = DATA_DIR / "cache"
    DATA_CHUNK_DIR: Path = DATA_DIR / "chunks"
    
    # FastAPI 
    PORT: int = 8000
    DEBUG: bool = False
    
    # Web Crawler
    CRAWLER_TIMEOUT_SECONDS: int = 30 # seconds
    CRAWLER_NETWORK_IDLE_TIMEOUT_MS: int = 5000 # milliseconds
    MAX_CONCURRENT_DOWNLOADS: int = 5
    
    # LlamaParse
    LLAMA_CLOUD_API_KEY: str
    LLAMA_CLOUD_TIER: Literal["fast", "cost_effective", "agentic", "agentic_plus"] = "fast"
    LLAMA_CLOUD_CONCURRENCY: int = 5
    
    # Ingestion Pipeline
    INGESTION_FOLDER: Literal["all", "curriculum", "information", "announcement"] = "curriculum"
    QDRANT_COLLECTION_NAME: str = "hcmus_documents"
    INGESTION_FORCE: bool = False
    INGESTION_BATCH_SIZE: int = 32
    INGESTION_SKIP_PARSE: bool = True
    INGESTION_SKIP_CHUNK: bool = False
    INGESTION_SKIP_UPLOAD: bool = False

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

    TEI_URL: str
    QDRANT_API: str
    QDRANT_URL: str
    GOOGLE_API_KEY: str
    
settings = Settings()

