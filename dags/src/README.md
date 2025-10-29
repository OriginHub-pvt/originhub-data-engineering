## DAGS/src Module Reference

This directory owns every helper that powers the Airflow pipeline pictured below. The workflow currently fetches RSS feeds, stores the raw XML, normalizes the feed items, filters them with an ML model, and scrapes the surviving URLs. Upcoming work will add a summarization task and a final writer that pushes summarized content to a vector database (VDB).

```
fetch_rss_feeds ─┬─> store_rss_feeds
                  └─> normalize_feeds ──> filter_articles ──> scrape_web_content ──> summarize_task ──> VDB load
```

Every file in `dags/src/` participates in that flow. The sections below document each module in detail, explaining what it exports, which environment variables it reads, and how it contributes to the DAG.

---

### `__init__.py`

Central import surface that keeps the DAG definition clean. It re-exports:

- `fetch_rss_feeds`, `store_rss_feeds` from `fetch_store_rss.py`
- `normalize_feeds_to_gcs`, `normalize_latest_feeds` from `xml_to_json.py`
- `filter_articles` from `filter_articles.py`
- `WebScraper`, `scrape_url`, `scrape_web_content`, `scrape_all_from_json_files` from `scrapper.py`
- `summarize_record`, `serialize_json` from `summarize_functions.py`

This allows `dags/airflow.py` (and future DAGs) to simply `from src import ...` without juggling multiple module paths.

---

### `fetch_store_rss.py`

Purpose: Fetch RSS/Atom feeds and persist the raw XML to Google Cloud Storage.

Key pieces:

| Symbol | Description |
| --- | --- |
| `_normalize_urls`, `_slug_from_url`, `_build_filename` | Utility helpers for cleaning URLs and generating timestamped slugs such as `20251028032530_example.com_feed.xml`. |
| `_extract_config` | Reads feed URLs and request timeout from DAG params or `dag_run.conf`. |
| `fetch_rss_feeds` | Airflow task function. Performs HTTP GET for every feed URL, raises on failure, and returns `{"feeds": [{"rss_url", "content", "fetched_at"}, …]}` for downstream tasks. |
| `store_rss_feeds` | Airflow task function. Uploads each raw XML payload to `XML_BUCKET_NAME` using `airflow.providers.google.cloud.hooks.gcs.GCSHook`. Returns the list of `gs://…` URIs pushed to the bucket. |

Environment variables:

- `XML_BUCKET_NAME` (required) — GCS bucket for raw XML.
- `DEFAULT_TIMEOUT` (implicit 30 seconds) — can be overridden via DAG params.

Contribution to the DAG:

- `fetch_rss_feeds` is the first node in the pipeline.
- The output fans out in parallel to `store_rss_feeds` (archival) and `normalize_feeds_to_gcs` (parsing).

---

### `xml_to_json.py`

Purpose: Turn the raw XML content into a canonical JSON structure and publish it to GCS. Also provides local utilities for reprocessing feeds on disk.

Key pieces:

| Symbol | Description |
| --- | --- |
| `_strip_html`, `_first_url_from_links`, `_best_url`, `_best_created`, `_best_updated`, `_coalesce_description`, `normalize_entry` | Clean up feed entries, strip HTML, choose canonical URLs, and normalize timestamps (ISO-8601 UTC). |
| `parse_xml_content`, `parse_xml_file` | Use `feedparser` to decode RSS/Atom XML into Python dictionaries. |
| `dedupe_by_url` | Prevent duplicate entries (falls back to title + createdDate when the URL is missing). |
| `normalize_feeds_to_gcs` | Airflow task function. Parses each XML string from the `fetch_rss_feeds` payload, dedupes entries, and uploads JSON to `JSON_BUCKET_NAME`. Returns a list of `gs://…` URIs. |
| `normalize_all_feeds`, `normalize_latest_feeds` | Local/CLI helpers that operate on files in `./dags/rss_data/` and generate `.normalized.json` files for quick reprocessing or tests. |

Environment variables:

- `JSON_BUCKET_NAME` (required) — GCS bucket for normalized JSON feeds.

Contribution to the DAG:

- `normalize_feeds_to_gcs` sits on the second branch of the fan-out. Its output feeds the ML classifier (`filter_articles`) and keeps a high-quality history of normalized feeds in GCS.
- `normalize_latest_feeds` is still available for the unit tests and ad-hoc command-line runs.

---

### `filter_articles.py`

Purpose: Apply a transformer-based classifier to normalized feed items so that only relevant articles go through expensive scraping/summarization.

Key pieces:

| Symbol | Description |
| --- | --- |
| `load_model_from_gcs` | Downloads tokenizer/model weights from `gs://<MODEL_BUCKET>/<MODEL_NAME>/v<MODEL_VERSION>/` via `GCSHook`, caches under `/tmp`, and loads them with `transformers.AutoModelForSequenceClassification`. |
| `predict_relevance` | Concatenates `title` and `description`, tokenizes, and runs inference with `torch`. Keeps only entries where the predicted label is 1. |
| `filter_articles` | Airflow task function. Accepts either the dict payload from `normalize_feeds_to_gcs` (`{"items": [...]}`) or a raw list, loads the model, and returns the filtered list for the next node. |

Environment variables:

- `MODEL_BUCKET` (default `origin-hub_model-weights`)
- `MODEL_NAME` (default `slm_filter`)
- `MODEL_VERSION` (default `1`)
- `LOG_LEVEL`

Contribution to the DAG:

- Runs immediately after `normalize_feeds`, trimming the dataset to articles worth scraping and summarizing. The next task receives a much smaller, higher-quality list.

Dependencies:

- `torch`, `transformers`, `airflow.providers.google.cloud.hooks.gcs.GCSHook`

---

### `scrapper.py`

Purpose: Retrieve full web pages for each filtered article, extract metadata, plain text, links, and images, and optionally perform batch scraping from stored JSON files.

Key pieces:

| Symbol | Description |
| --- | --- |
| `WebScraper` | Class that handles HTTP requests (with SSL retry), HTML parsing via `BeautifulSoup`, metadata extraction, text cleaning, link/image resolution, and word counts. |
| `scrape_url` | Convenience wrapper to scrape a single URL with a fresh `WebScraper` instance. |
| `scrape_web_content` | Airflow task function. Expects a single URL via DAG params or `dag_run.conf`, returns a dict containing scraped content, metadata, and word counts. In the pipeline this sits after `filter_articles`. |
| `scrape_all_from_json_files` | Iterates over JSON files in `dags/filtered_data`, scrapes each URL, and writes a summary (`title`, `scraped_content`) to `dags/scraped_data/scraped_data.json`. Useful for bulk offline runs. |

Contribution to the DAG:

- `scrape_web_content` executes after `filter_articles`, providing the raw text that the future `summarize_task` will consume. It is the last current node in the flow before summarization/VDB storage is added.

Implementation notes:

- Uses a desktop-style User-Agent to avoid trivial blocking.
- Removes scripts/styles before text extraction to reduce noise.
- Resolves relative URLs for links and images.
- Handles per-item errors by logging and providing fallback content where possible.

---

### `summarize_functions.py`

Purpose: Provide reusable utilities for summarizing scraped articles and serializing the results before they are pushed to storage (e.g., a VDB).

Key pieces:

| Symbol | Description |
| --- | --- |
| `summarizer = pipeline("summarization", model="google/pegasus-cnn_dailymail")` | Pre-loaded Hugging Face summarization pipeline. |
| `summarize_record(record)` | Accepts a dict with a `content` key, chunks long text (1,000 characters by default), runs the summarizer on each chunk, stitches the results into a `summary` field, and returns the enriched record. |
| `serialize_json(data)` | Takes a list of dictionaries and converts it into a `BytesIO` object containing pretty-printed JSON—convenient for uploading to GCS or S3 directly. |

Contribution to the DAG:

- Will be used by the forthcoming `summarize_task` (inserted between `scrape_web_content` and the VDB writer). Summaries generated here can be passed downstream to the VDB loader without re-summarizing in that task.

Dependencies:

- `transformers`

---

### `README.md` (this document)

- The canonical reference for everything living in `dags/src`. Update it whenever new helper modules or Airflow tasks are added so the DAG’s responsibilities remain transparent.

---

### Putting It All Together

1. `fetch_rss_feeds` downloads RSS/Atom feeds; the payload flows to both archival and parsing branches.
2. `store_rss_feeds` uploads raw XML files to `gs://<XML_BUCKET_NAME>/…`, preserving an unmodified record of the feed.
3. `normalize_feeds_to_gcs` converts the same payload to tidy JSON and writes it to `gs://<JSON_BUCKET_NAME>/…`.
4. `filter_articles` loads the ML classifier from GCS, filters the normalized entries, and pushes only relevant articles forward.
5. `scrape_web_content` fetches the full HTML for each remaining article, cleaning it for later summarization.
6. `summarize_task` will call helpers in `summarize_functions.py` to add concise summaries.
7. final node writes the summaries into a vector database or other target store for retrieval.

This modular design keeps network I/O, parsing, ML inference, scraping, and future summarization/storage concerns isolated while still composing into a single DAG. When you wire up the new tasks, simply import from `src` and follow the patterns already in place.
