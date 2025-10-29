# OriginHub Data Engineering

This repository houses the data-ingestion and enrichment pipeline behind OriginHub. It packages an Apache Airflow stack (running in Docker) that collects RSS feeds, normalizes the results, filters them with an ML model, scrapes full articles, and prepares the content for downstream summarization and vector-database storage.

Everything you need to reproduce the environment—from Docker configuration to helper modules, tests, and labeling utilities—lives here. This README walks through every moving piece so you can run, extend, and maintain the project with confidence.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Repository Layout](#repository-layout)
3. [Airflow DAG & Data Flow](#airflow-dag--data-flow)
4. [Module Reference (`dags/src`)](#module-reference-dagssrc)
5. [Environment & Configuration](#environment--configuration)
6. [Building & Running the Stack](#building--running-the-stack)
7. [Operating the Pipeline](#operating-the-pipeline)
8. [Supporting Tools](#supporting-tools)
9. [Testing](#testing)
10. [Troubleshooting & Tips](#troubleshooting--tips)
<!-- 11. [Roadmap](#roadmap) -->

---

## Architecture Overview

The ingestion stack is based on Apache Airflow and runs inside Docker containers. A custom Airflow image (built from the supplied `Dockerfile`) bundles all Python dependencies—including `transformers`, `torch`, `feedparser`, and `weaviate-client`.

Two DAGs orchestrate the processing:

1. **`rss_feed_ingestion_and_normalization`** (ingestion DAG)

```
RSS feeds ──> fetch_rss_feeds ─┬─> store_rss_feeds (raw XML -> GCS)
                               └─> normalize_feeds (JSON -> GCS)
                                     │
                                     ▼
                                filter_articles (ML relevance model)
                                     │
                                     ▼
                                scrape_web_content (HTML -> text, triggers DAG #2 per article)
```

2. **`summarize_and_store_weaviate`** (enrichment DAG, triggered on-demand)

```
incoming article payload ──> summarize_record ──> store_in_weaviate (vector DB)
```

When the ingestion DAG scrapes a filtered article, it uses Airflow’s execution API to trigger the summarization DAG with the scraped payload. That second DAG produces abstractive summaries and pushes them into Weaviate in near-real time. Supporting utilities include:

- A Streamlit labeling tool (`labeling/`) for generating ML training data.
- Weaviate smoke-test scripts for verifying vector DB connectivity.
- Pytest suites that exercise the fetch/parse/filter/scraper modules.

---

## Repository Layout

| Path | Purpose |
| --- | --- |
| `dags/` | Airflow DAG definitions plus helper packages under `dags/src`. |
| `dags/src/` | Core Python modules used by the DAG (fetching, normalization, filtering, scraping, summarization helpers). Documented fully in `dags/src/README.md`. |
| `config/`, `logs/`, `plugins/` | Airflow runtime directories (created/reset by `setup.sh`). |
| `Dockerfile` | Builds a custom Airflow image with all Python dependencies preinstalled. |
| `docker-compose.yaml` | Orchestrates the Airflow stack (scheduler, webserver, worker, triggerer, dag-processor, API server, Postgres, Redis). |
| `.env` | Environment overrides (bucket names, credentials, API keys, etc.). **Do not commit secrets.** |
| `requirements.txt` | Python dependency manifest (mirrors what the Dockerfile installs). |
| `setup.sh` | Convenience script to rebuild and restart the stack from scratch. |
| `labeling/` | Streamlit app (`labeler_app.py`) used for manual relevance labeling. |
| `tests/` | Pytest suite covering RSS ingestion, XML normalization, scraper utilities, and the labeling app backend. |
| `simple_weaviate_test.py`, `weaviate_testing.py` | Quick scripts for validating Weaviate connectivity (used in upcoming VDB integration). |
| `volumes/` | Placeholder for mounted volumes (when required). |

---

## Airflow DAG & Data Flow

### DAG #1 – `rss_feed_ingestion_and_normalization`

1. **`fetch_rss_feeds`** – Pulls RSS/Atom feeds, returning a payload of raw XML strings plus metadata.
2. **`store_rss_feeds`** – Uploads each XML document to `XML_BUCKET_NAME` in GCS for archival. Runs in parallel with step 3.
3. **`normalize_feeds`** – Converts XML to JSON (deduped, cleaned, normalized) and writes to `JSON_BUCKET_NAME`.
4. **`filter_articles`** – Loads the ML relevance model from `MODEL_BUCKET` and keeps only high-signal stories.
5. **`scrape_web_content`** – Scrapes each remaining URL, assembles an enriched record, and triggers DAG #2 (`summarize_and_store_weaviate`) for every article via the Airflow execution API. Returns metrics (`triggered` / `failed`).

### DAG #2 – `summarize_and_store_weaviate`

1. **`summarize_record`** – Uses the Hugging Face summarizer (Pegasus) to generate an abstractive summary for the article payload received from DAG #1.
2. **`store_in_weaviate`** – Ensures the `ArticleSummary` schema exists and writes the summary/title into Weaviate for downstream semantic search.

Trigger config for DAG #1:

```bash
docker compose run --rm airflow-cli \
  airflow dags trigger rss_feed_ingestion_and_normalization \
  --conf '{
    "rss_url": "https://feeds.arstechnica.com/arstechnica/index/"
  }'
```

Once triggered, raw and normalized artifacts appear in the configured GCS buckets, filtered articles progress to scraping, and logs for each step are captured in the Airflow UI and `logs/`.

---

## Module Reference (`dags/src`)

Every helper module is documented in `dags/src/README.md`. Highlights:

- `fetch_store_rss.py` – RSS fetching and XML archival (GCS).
- `xml_to_json.py` – XML parsing, deduping, JSON upload, plus local normalization utilities.
- `filter_articles.py` – ML relevance scoring using Hugging Face transformers weights stored in GCS.
- `scrapper.py` – HTML fetching, metadata extraction, bulk scraping utilities, and the `scrape_web_content` Airflow task that triggers the second DAG for each article.
- `summarize_functions.py` – Summarization helpers leveraged by `summarize_record` inside the enrichment DAG.
- `weaviate_store.py` – Connection/session management and schema enforcement for storing summaries in Weaviate via `store_in_weaviate`.
- `__init__.py` – Re-exports the public symbols for clean imports.

Refer to that README for the full API surface and environment-variable requirements per module.

---

## Environment & Configuration

Create a `.env` file in the repository root. **Do not commit real secrets.** Use the template below and replace placeholders with your values:

```env
# Required by the Airflow Docker stack
AIRFLOW_UID=$(id -u)

# GCS buckets
XML_BUCKET_NAME=xml-temp-storage
JSON_BUCKET_NAME=json-train-storage
MODEL_BUCKET=origin-hub_model-weights
MODEL_NAME=slm_filter
MODEL_VERSION=1

# Hugging Face summarization (used by summarize_functions.py)
HUGGINGFACE_APIKEY=replace_with_your_token

# Vector DB (future integration)
WEAVIATE_URL=http://localhost:8081

# Airflow connection for Google Cloud
# Generate by URL-encoding your service-account JSON:
# python - <<'PY'
# import json, urllib.parse
# print(urllib.parse.quote(open("sa.json").read()))
# PY
AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=google-cloud-platform://?extra__google_cloud_platform__keyfile_dict=%7B...encoded JSON...%7D
```

**Service account tips**

- The Airflow connection uses the execution API, so the JSON must be URL encoded and injected via `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`.
- Ensure the service account has `storage.objectAdmin` on the XML/JSON/Model buckets.
- Keep the original JSON outside the repo (e.g., `./config/gcp-sa.json`) if you need to mount it for other tooling.

**Model assets**

- Expected to live in `gs://<MODEL_BUCKET>/<MODEL_NAME>/v<MODEL_VERSION>/`.
- The classifier is loaded with Hugging Face’s `AutoModelForSequenceClassification`. Adjust ENV vars to swap models/versions.

---

## Building & Running the Stack

Prerequisites:

- Docker Desktop (or Docker Engine) with at least 6 GB memory allocated.
- Docker Compose v2 (`docker compose` CLI).
- Internet access (for pulling images and Python packages).

Steps:

1. **Clone the repo**
   ```bash
   git clone https://github.com/OriginHub/originhub-data-engineering.git
   cd originhub-data-engineering
   ```
2. **Set up environment**
   - Copy `.env.example` (if provided) or create `.env` using the template above.
   - Confirm `Dockerfile` includes all required Python packages (already configured).
3. **Build the custom Airflow image**
   ```bash
   docker compose build
   ```
4. **Initialize Airflow metadata DB and install requirements**
   ```bash
   docker compose up airflow-init
   ```
5. **Start the full stack**
   ```bash
   docker compose up -d
   ```
6. **Access the UI**
   - Webserver: http://localhost:8080
   - Default credentials: `airflow` / `airflow`
   - Unpause `rss_feed_ingestion_and_normalization` to begin scheduling.

**Shortcut:** `./setup.sh` performs `docker compose down -v`, rebuilds the image, recreates Airflow directories, and brings the stack back up.

---

## Operating the Pipeline

1. Populate `.env` with bucket names, model metadata, and credentials.
2. Start the stack and verify the DAG appears in the UI (no import errors).
3. Trigger the DAG manually (UI or CLI). Provide RSS URLs in `conf`.
4. Monitor progress in the graph view. Logs for each task are accessible via the UI or in the `logs/` directory.
5. Validate outputs:
   - Raw XML → `gs://<XML_BUCKET_NAME>/rss/xml/…`
   - Normalized JSON → `gs://<JSON_BUCKET_NAME>/rss/json/…`
   - Filter metrics → `scrape_web_content` logs (shows how many summarization DAGs were triggered).
   - Summaries → stored as Weaviate objects in the `ArticleSummary` class (check via Weaviate console, REST API, or client scripts).
6. The summarization DAG (`summarize_and_store_weaviate`) is automatically triggered by `scrape_web_content`. You can also run it manually via:
   ```bash
   docker compose run --rm airflow-cli \
     airflow dags trigger summarize_and_store_weaviate \
     --conf '{"article": {"title": "Example", "scraped_content": "..."}}'
   ```

---

## Supporting Tools

### Manual Labeling App (`labeling/`)

- Streamlit application (`labeling/labeler_app.py`) for tagging JSON records as relevant or irrelevant.
- Usage:
  ```bash
  cd labeling
  pip install -r requirements.txt
  streamlit run labeler_app.py
  ```
- Outputs include labeled JSON suitable for training the relevance classifier used in `filter_articles.py`.

### Weaviate Test Scripts

- `simple_weaviate_test.py` and `weaviate_testing.py` provide quick checks for a running Weaviate instance.
- Helpful when wiring the upcoming VDB persistence task.

---

## Testing

The `tests/` directory contains Pytest suites:

| Test file | Focus |
| --- | --- |
| `tests/unit_tests/test_fetch_store_rss.py` | URL normalization + fetch/store behaviour. |
| `tests/unit_tests/test_xml_to_json.py` | XML parsing, dedupe logic, local normalization helpers. |
| `tests/unit_tests/test_scrapper.py` | HTML scraping utilities and text extraction. |
| `tests/unit_tests/test_labeler_app.py` | Streamlit labeling back-end functions. |

Running tests locally:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r tests/requirements.txt  # if additional deps are introduced
pytest
```

---

## Troubleshooting & Tips

- **DAG missing in UI** – Check `docker compose logs airflow-scheduler` and `airflow-dag-processor` for import errors (missing dependencies, bad env vars, etc.).
- **`ModuleNotFoundError` for Airflow packages** – Ensure you ran `docker compose build` followed by `docker compose up airflow-init`.
- **GCS connection errors** – Confirm `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT` is set correctly and the service account has bucket permissions.
- **XCom size issues** – Avoid returning huge payloads from tasks; write large artifacts to GCS and return pointers instead.
- **Transformer weights** – Large models load from GCS. Ensure the bucket contains the full tokenizer/model files and that the container has sufficient memory (4 GB+ recommended).
- **Weaviate connectivity** – Update `WEAVIATE_URL` in `.env` and use the provided test scripts to verify before wiring into the DAG.

<!-- ---

## Roadmap

- **Summarization task** – Use `summarize_functions.summarize_record` to generate abstractive summaries immediately after scraping.
- **Vector database writer** – Push summarized articles into Weaviate using `weaviate-client` and the configured `WEAVIATE_URL`.
- **Monitoring & metrics** – Add Airflow SLAs/alerts and statsd instrumentation.
- **Automated labeling feedback loop** – Integrate the Streamlit labeling app outputs into the ML training pipeline.
- **CI/CD** – Add automated tests (GitHub Actions) to guard against regressions.

--- -->

Feel free to reach out or open issues if you extend the pipeline. Keep `dags/src/README.md` in sync when new modules or tasks are introduced, and remember to rotate credentials in `.env` whenever you onboard new environments.
