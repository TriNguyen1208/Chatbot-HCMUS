# Scraper Management Architecture & Flow Guide

## Scraper Management Architecture & Flow Guide
This component manages the lifecycle, execution, and data storage of web scrapers within the HCMUS Chatbot pipeline. It physically resides under the `backend/pipeline/scrapers/`, `backend/pipeline/downloader.py`, and `backend/execution/run_crawler.py` locations, orchestrating asynchronous requests and Playwright browser instances to safely gather and save curriculum documents.

### 🔄 Core Logic Flow

The flow chart below illustrates the end-to-end execution sequence of the crawler engine at runtime:

```
[run_crawler.py]
       │
       ▼ (asyncio.run(main))
┌───────────────────────────────────────────┐
│ ScraperManager.scrape_all()               │
└────────────────────┬──────────────────────┘
                     │ (Iterates registry)
                     ▼
┌───────────────────────────────────────────┐
│ BaseScraper.__aenter__()                  │
│ - Launches Headless Playwright Chromium   │
│ - Instantiates HTTPX AsyncClient Session  │
└────────────────────┬──────────────────────┘
                     │
                     ▼
┌───────────────────────────────────────────┐
│ CurriculumScraper.scrape()                │
│ - Navigates to Curriculum Portal URL      │
│ - Extracts and rewires program list Links │
│ - Iterates links; extracts text           │
│ - Launches parallel attachment downloads  │
└──────────────┬───────────────────┬────────┘
               │ (Yields Text)     │ (Yields PDF/Bin)
               ▼                   ▼
┌───────────────────────────────────────────┐
│ ScraperManager._process_single_file()     │
│ - Enforces Max Concurrent Downloads       │
└────────────────────┬──────────────────────┘
                     │
                     ▼
┌───────────────────────────────────────────┐
│ DocumentDownloader.store_document()       │
│ - Performs SHA-256 Hashing on raw bytes   │
│ - Writes distinct file to disk if unique  │
└────────────────────┬──────────────────────┘
                     │ (Returns file metadata)
                     ▼
┌───────────────────────────────────────────┐
│ ScraperManager._append_manifest()         │
│ - Enters asyncio.Lock to avoid conflicts  │
│ - Appends entry to central manifest.json  │
└───────────────────────────────────────────┘
```

#### Step-by-Step Execution Sequence
1. **Engine Startup**: The application executes [run_crawler.py](../execution/run_crawler.py), which sets up standard logging and instantiates the [ScraperManager](../pipeline/scrapers/scraper_manager.py) class.
2. **Scraper Class Registry**: Subclasses of [BaseScraper](../pipeline/scrapers/base_scraper.py) (e.g., [CurriculumScraper](../pipeline/scrapers/curriculum_scraper.py)) are automatically registered in the class registry using python's `__init_subclass__` hook.
3. **Context Entry**: The manager opens the scraper in an async context block (`async with`), triggering Chromium initialization in Playwright and opening an HTTP client session.
4. **Data Generation**: The scraping process scans portal anchor tags, extracts programs' information as UTF-8 encoded text files, and fetches PDF/binary attachments using concurrent HTTP GET tasks.
5. **Storage Pipeline**: The scraped data stream is received by `_process_single_file` inside [ScraperManager](../pipeline/scrapers/scraper_manager.py). It limits raw write traffic utilizing `asyncio.Semaphore` set by configuration limits.
6. **Deduplication Check**: The [DocumentDownloader](../pipeline/downloader.py) hashes file bytes (SHA-256), checking if the file hash prefix matches an existing disk path to avoid duplicate operations.
7. **Manifest Update**: A thread/task-safe lock is acquired, and the metadata (hash, paths, timestamp) is added to `manifest.json`.

---

### ⚙️ Component Blueprint

| Component / Method | Type | Input/Output | Primary Responsibility |
| :--- | :--- | :--- | :--- |
| **`BaseScraper`** | Abstract Class | N/A | Base class using `__init_subclass__` for auto-registering scraper engines and establishing base context configurations. |
| `BaseScraper.__aenter__` | Async Method | None -> `BaseScraper` | Launches a headless Chromium browser instance and sets up the shared HTTPX async client session. |
| `BaseScraper.__aexit__` | Async Method | Exceptions metadata -> None | Safely closes open browser pages, the Playwright context, browser instances, and client sessions. |
| `BaseScraper.scrape` | Async Abstract Generator | None -> `AsyncIterator[tuple[str, bytes]]` | Enforces implementation of site-specific crawl generators yielding tuple pairs of `(filename, bytes)`. |
| **`CurriculumScraper`** | Class | N/A | Dedicated subclass for extracting HTML text content and associated PDF curriculum plans from the educational program portal. |
| `CurriculumScraper.scrape` | Async Generator | None -> `AsyncIterator[tuple[str, bytes]]` | Crawls portal program links, parses HTML contents, and concurrently extracts pdf attachments. |
| `CurriculumScraper._extract_attachment` | Async Method | URL, filename -> `tuple[str, bytes] \| None` | Downloads attachment binary bytes using the HTTP client session. |
| `CurriculumScraper._attachment_filename` | Method | URL, filename, content type -> `str` | Cleanly extracts or infers valid filenames, falling back to guessed extensions when necessary. |
| **`ScraperManager`** | Class | N/A | Top-level crawler engine director managing concurrency controls, scraper invocations, and manifests. |
| `ScraperManager.scrape` | Async Method | Site Name -> None | Retrieves scraper class from registry, enters its context, starts the generator, and spawns concurrent file tasks. |
| `ScraperManager.scrape_all` | Async Method | None -> None | Triggers sequential scraping tasks for all registered crawlers in the system. |
| `ScraperManager._process_single_file` | Async Method | Filename, bytes, site name -> None | Handles semaphore acquisition and delegates raw file writes to the downloader before manifest updates. |
| `ScraperManager._append_manifest` | Method | Site name, metadata -> None | Enforces sequential execution using a lock to write file metadata to `manifest.json`. |
| **`DocumentDownloader`** | Class | N/A | High-level file saver responsible for file-path management and content deduplication. |
| `DocumentDownloader.store_document` | Async Method | Filename, bytes, site name -> `dict[str, Any]` | Hashes bytes, saves new content variants to disk, and outputs structural metadata. |
