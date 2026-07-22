# Backend Service Deployment Guide

This directory contains the core crawling pipeline, documentation logs, and API engines. Follow these steps to set up and run the backend components locally.

## 🛠️ Environment Prerequisites

### 1. Navigate to the Backend Root
All subsequent commands must be executed from within the `backend/` directory to ensure relative paths and modules resolve correctly:
```bash
cd backend
```

### 2a. Create Python Virtual Environment
Before running any installation or execution scripts, ensure your virtual environment is active in your terminal shell:
```powershell
# Windows (PowerShell)
python -m venv .venv

# Linux / macOS
python3 -m venv .venv
```

### 2b. Activate the Virtual Environment
Before running any installation or execution scripts, ensure your virtual environment is active in your terminal shell:
```powershell
# Windows (PowerShell)
.\.venv\Scripts\activate

# Linux / macOS
source .venv/bin/activate
```

### 3. Dependency Synchronisation
Install the necessary package versions logging tracked modules into your setup environment:

```bash
pip install -r requirements.txt
playwright install chromium
```

## 🚀 Execution & Command Reference

### Run the FastAPI Server
To launch the backend API endpoints locally, run the uvicorn development worker. It will load the application instance and log the initialization status:

```bash
<updating>
```

### Execute the Scraper Engine Pipeline
To manually boot the web scraping worker layer to collect latest curriculum targets and download attachments directly, execute the script via python module syntax:

```bash
python -m execution.run_crawler
```

### Execute the Ingestion Pipeline
To process the collected files from the raw storage data lake, extract document layouts, chunk text structures, generate embeddings, and sync them to your vector storage, execute the ingestion engine:

```bash
python -m execution.run_ingestion
```