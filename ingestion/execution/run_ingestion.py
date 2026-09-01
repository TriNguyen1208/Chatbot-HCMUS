import logging
import sys
import asyncio

from ingestion.logger_config import setup_logging
from ingestion.config.settings import settings
from ingestion.pipeline import Parser, Chunker, QdrantVectorDB

setup_logging(level=logging.INFO)
logger = logging.getLogger(__name__)


async def run_ingestion():
    """
    Execute the multi-stage document ingestion pipeline based on settings.py configurations:
      Phase 1: Parse raw documents into Markdown format (LlamaCloud).
      Phase 2: Chunk Markdown documents into JSONL structured chunks.
      Phase 3: Generate embeddings and upsert chunks into Qdrant Vector DB.
    """
    skip_parse = settings.INGESTION_SKIP_PARSE
    skip_chunk = settings.INGESTION_SKIP_CHUNK
    skip_upload = settings.INGESTION_SKIP_UPLOAD

    logger.info("Starting Document Ingestion Pipeline...")

    # Phase 1: Parse Raw Documents -> Markdown
    if not skip_parse:
        logger.info("Phase 1: Parsing raw documents...")
        try:
            parser = Parser()
            await parser.parse_all()
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
            chunker.chunk_all(force=settings.INGESTION_FORCE)
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
            logger.info(f"Initializing Qdrant collection '{settings.QDRANT_COLLECTION_NAME}'...")
            db.init_collection(collection_name=settings.QDRANT_COLLECTION_NAME)

            logger.debug(f"Uploading chunk files for '{settings.INGESTION_FOLDER}' to collection '{settings.QDRANT_COLLECTION_NAME}'...")
            db.upload_jsonl_folder(
                collection_name=settings.QDRANT_COLLECTION_NAME,
                folder=settings.INGESTION_FOLDER,
                batch_size=settings.INGESTION_BATCH_SIZE,
            )
            logger.info("Phase 3 (Vector DB Ingestion) completed successfully.")
        except Exception as e:
            logger.error(f"Phase 3 (Vector DB Ingestion) failed: {e}", exc_info=True)
            sys.exit(1)
    else:
        logger.info("Phase 3: Vector DB Ingestion skipped.")

    logger.info("Document Ingestion Pipeline completed.")



if __name__ == "__main__":
    asyncio.run(run_ingestion())

