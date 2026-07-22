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
    
    # FastAPI 
    PORT: int = 8000
    DEBUG: bool = False
    
    # Web Crawler
    CRAWLER_TIMEOUT_SECONDS: int = 30 # seconds
    CRAWLER_NETWORK_IDLE_TIMEOUT_MS: int = 5000 # milliseconds
    MAX_CONCURRENT_DOWNLOADS: int = 5
    
    # LlamaParse
    LLAMA_CLOUD_API_KEY: str
    LLAMA_CLOUD_TIER: Literal["fast", "cost_effective", "agentic", "agentic_plus"] = "cost_effective"
    LLAMA_CLOUD_MAX_WORKERS: int = 5
    
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )
    
settings = Settings()
