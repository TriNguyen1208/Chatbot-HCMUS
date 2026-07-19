# Document Parsing Architecture & Flow Guide

## Document Parsing Architecture & Flow Guide
This component provides utilities to parse unstructured documents (PDFs, images, Excel sheets, Word documents, PowerPoint presentations) into structured Markdown format using the LlamaCloud parsing API. Located strictly in `backend/pipeline/parser.py`, it serves as a core translation layer in the data ingestion pipeline, transforming raw binaries into clean markdown suited for LLM context injection.

### 🔄 Core Logic Flow

The flow chart below illustrates the control flow of synchronous and asynchronous file parsing tasks at runtime:

```
    [FileParser.toMd()] (Single File)           [FileParser.toMdBatch()] (Batch)
                 │                                             │
                 │ (asyncio.run)                               │ (asyncio.run)
                 ▼                                             ▼
       ┌───────────────────┐                        ┌──────────────────────┐
       │   single_task()   │                        │   parallel_tasks()   │
       └─────────┬─────────┘                        │ - Instantiates       │
                 │                                  │   Semaphore(5)       │
                 │                                  └──────────┬───────────┘
                 │                                             │ (Spawns Tasks)
                 ▼                                             ▼
       ┌───────────────────────────────────────────────────────────────────┐
       │                     _parse_with_semaphore()                       │
       │                     - Acquires Semaphore Lock                     │
       └─────────────────────────────────┬─────────────────────────────────┘
                                         │
                                         ▼
       ┌───────────────────────────────────────────────────────────────────┐
       │                    _parse_single_file_async()                     │
       │                    - Initializes client context                   │
       └─────────────────────────────────┬─────────────────────────────────┘
                                         │
                                         ▼
       ┌───────────────────────────────────────────────────────────────────┐
       │                   LlamaCloud.files.create()                       │
       │                   - Uploads raw document binary                   │
       └─────────────────────────────────┬─────────────────────────────────┘
                                         │ (Returns file ID)
                                         ▼
       ┌───────────────────────────────────────────────────────────────────┐
       │                   LlamaCloud.parsing.parse()                      │
       │                   - Executes parse task on cloud (Fast/Agentic)   │
       └─────────────────────────────────┬─────────────────────────────────┘
                                         │ (Returns raw page markdown data)
                                         ▼
       ┌───────────────────────────────────────────────────────────────────┐
       │                        Markdown Assembly                          │
       │                        - Concatenates page markdown strings       │
       │                        - Encodes & saves to file output path      │
       └───────────────────────────────────────────────────────────────────┘
```

#### Step-by-Step Execution Sequence
1. **Invocation**: Developers invoke either [toMd](file:///d:/01_Personal/02_Works/Project/AI_Project/ChatUS/Chatbot-HCMUS/backend/pipeline/parser.py#L44) or [toMdBatch](file:///d:/01_Personal/02_Works/Project/AI_Project/ChatUS/Chatbot-HCMUS/backend/pipeline/parser.py#L68) using local file system paths.
2. **Event Loop Initialization**: Synchronous entry points create an asynchronous context with `asyncio.run()`, spinning up a temporary event loop.
3. **Throttling Lock**: If batching, tasks run through the `_parse_with_semaphore` helper wrapper. It enforces a strict concurrency threshold (defined by `MAX_WORKERS = 5`) to prevent hitting API rate limits.
4. **Cloud Upload**: Files are sent to the LlamaCloud file registry using `client.files.create(purpose='parse')`.
5. **Remote Parsing**: The parser triggers the remote parsing job, requesting a specific tier (e.g., `"cost_effective"`) and requesting the response format expand into markdown representation.
6. **Local Aggregation**: Upon successful return, pages are joined using standard horizontal rule dividers (`\n\n---\n\n`), written as UTF-8 encoded text to the final target file.

---

### ⚙️ Component Blueprint

| Component / Method | Type | Input/Output | Primary Responsibility |
| :--- | :--- | :--- | :--- |
| **`FileParser`** | Class | N/A | Namespace container for processing local documents through the LlamaCloud remote parsing endpoints. |
| `FileParser._parse_single_file_async` | Async Static Method | `client`, `input_path`, `output_path`, `tier` -> None | Performs the low-level API pipeline calls, aggregates multiline page markdown text, and writes the output file. |
| `FileParser.toMd` | Static Method | `input_path`, `output_path`, `tier` -> None | Bootstraps event loops and runs a single file parsing task synchronously. |
| `FileParser._parse_with_semaphore` | Async Static Method | `client`, `input_path`, `output_path`, `tier`, `semaphore` -> None | Locks execution within the specified concurrent semaphore constraints before executing parsing routines. |
| `FileParser.toMdBatch` | Static Method | `file_pairs`, `tier` -> None | Sets up parallel tasks, coordinates semaphore bounds, and triggers concurrent file parses using `asyncio.gather()`. |

---

### 📁 Output Destination
Save your output files using descriptive UPPERCASE snake_case names (e.g., `SCRAPER_MANAGEMENT_FLOW.md`) inside the centralized `backend/docs/` folder. Remember that the `docs/` directory resides strictly inside the root of the `backend/` workspace.
