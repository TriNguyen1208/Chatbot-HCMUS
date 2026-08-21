import logging
import sys

from ingestion.logger_config import setup_logging
from ingestion.config.settings import settings
from ingestion.pipeline.parser.parser import FileParser
from ingestion.pipeline.chunker import Chunker
from ingestion.pipeline.database.db import QdrantVectorDB

setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_ingestion():
    """
    Execute the multi-stage document ingestion pipeline based on settings.py configurations:
      Phase 1: Parse raw documents into Markdown format (LlamaCloud).
      Phase 2: Chunk Markdown documents into JSONL structured chunks.
      Phase 3: Generate embeddings and upsert chunks into Qdrant Vector DB.
    """
    folder = settings.INGESTION_FOLDER
    collection_name = settings.QDRANT_COLLECTION_NAME
    force = settings.INGESTION_FORCE
    batch_size = settings.INGESTION_BATCH_SIZE
    tier = settings.LLAMA_CLOUD_TIER
    skip_parse = settings.INGESTION_SKIP_PARSE
    skip_chunk = settings.INGESTION_SKIP_CHUNK
    skip_upload = settings.INGESTION_SKIP_UPLOAD

    logger.info("Starting Document Ingestion Pipeline...")
    logger.info(
        f"Config: folder='{folder}', collection='{collection_name}', "
        f"force={force}, batch_size={batch_size}, tier='{tier}'"
    )

    # Phase 1: Parse Raw Documents -> Markdown
    if not skip_parse:
        logger.info("Phase 1: Parsing raw documents...")
        try:
            FileParser.parse_folder(folder=folder, force=force, tier=tier)
            logger.info("Phase 1 (Parsing) completed successfully.")
        except Exception as e:
            logger.error(f"Phase 1 (Parsing) failed: {e}", exc_info=True)
            if not skip_chunk or not skip_upload:
                logger.warning("Proceeding with subsequent phases using existing data...")
    else:
        logger.info("Phase 1: Parsing skipped.")

    # Phase 2: Chunk Markdown Documents -> JSONL
    if not skip_chunk:
        logger.info("Phase 2: Chunking markdown documents...")
        try:
            chunker = Chunker()
            chunker.chunk_md_folder(folder=folder, force=force)
            logger.info("Phase 2 (Chunking) completed successfully.")
        except Exception as e:
            logger.error(f"Phase 2 (Chunking) failed: {e}", exc_info=True)
            if not skip_upload:
                logger.warning("Proceeding with database upload using existing chunks...")
    else:
        logger.info("Phase 2: Chunking skipped.")

    # Phase 3: Vector Database Ingestion (Qdrant)
    if not skip_upload:
        logger.info("Phase 3: Ingesting vectors into Qdrant...")
        try:
            db = QdrantVectorDB()
            logger.info(f"Initializing Qdrant collection '{collection_name}'...")
            db.init_collection(collection_name=collection_name)

            logger.info(f"Uploading chunk files from folder '{folder}' to collection '{collection_name}'...")
            db.upload_jsonl_folder(
                collection_name=collection_name,
                folder=folder,
                batch_size=batch_size,
            )
            logger.info("Phase 3 (Vector DB Ingestion) completed successfully.")
        except Exception as e:
            logger.error(f"Phase 3 (Vector DB Ingestion) failed: {e}", exc_info=True)
            sys.exit(1)
    else:
        logger.info("Phase 3: Vector DB Ingestion skipped.")

    logger.info("Document Ingestion Pipeline completed.")



if __name__ == "__main__":
    run_ingestion()

