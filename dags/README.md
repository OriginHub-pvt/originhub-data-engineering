# RSS Feed Ingestion and Normalization DAG

This project ships an Apache Airflow DAG named `rss_feed_ingestion_and_normalization` that fetches one or more RSS/Atom feeds, stores the raw XML to a local folder, and normalizes them into a unified JSON schema. The DAG is tagged with `rss`, `ingestion`, and `normalization`, so you can quickly filter for it inside the Airflow UI.

## Prerequisites

- Docker and Docker Compose installed locally.
- (macOS/Linux) `AIRFLOW_UID` exported so files created in the containers stay writable:
  ```bash
  export AIRFLOW_UID=$(id -u)
  ```

## How to run AirFlow Project

From the project root:

```bash
./setup.sh
docker compose up -d
```

The scheduler, webserver, worker, and supporting services will start in the background. To stop everything later, run `docker compose down`.

## Access the Airflow UI

- URL: http://localhost:8080  
- Default credentials: `airflow` / `airflow`

Use the search box or the `rss` tag to find the `rss_feed_ingestion_and_normalization` DAG. Unpause it if needed.

## DAG Workflow

The DAG consists of three sequential tasks:
1. **fetch_rss_feeds**: Fetches RSS/Atom feed content from provided URLs
2. **store_rss_feeds**: Saves the raw XML feed content to the local filesystem
3. **normalize_feeds**: Converts XML feeds into normalized JSON format with a unified schema

## Trigger the DAG

You can trigger from the UI (`Trigger DAG w/ config`) or the CLI. The DAG accepts the following optional parameters:

- `rss_url`: list of feed URLs (string, comma/newline separated list, or JSON array)
- `output_dir`: target directory relative to `/opt/airflow/dags` for storing XML files (defaults to `rss_data`)
- `request_timeout`: HTTP timeout in seconds (default `30`)

Example JSON config:

```json
{
  "rss_url": "https://feeds.arstechnica.com/arstechnica/index/",
  "output_dir": "rss_data"
}
```

To trigger via CLI from the host machine:

```bash
docker compose run --rm airflow-cli \
  airflow dags trigger rss_feed_ingestion_and_normalization \
  --conf '{"rss_url": "https://feeds.arstechnica.com/arstechnica/index/"}'
```

## Output

The DAG produces two types of output:

### Raw XML Feeds

Fetched feeds are stored under `dags/rss_data/` (or the directory you override with `output_dir`). Each file is timestamped and suffixed with a slug derived from the feed URL, for example:

```
./dags/rss_data/20251026002601_feeds.arstechnica.com_arstechnica_index.xml
```

### Normalized JSON Feeds

Normalized feeds are stored under `dags/normalized_feeds/` with the same timestamp and slug as the XML file, but with a `.normalized.json` extension:

```
./dags/normalized_feeds/20251026002601_feeds.arstechnica.com_arstechnica_index.normalized.json
```

Each normalized JSON file contains an array of feed entries with a unified schema:
```json
[
  {
    "title": "Article Title",
    "url": "https://example.com/article",
    "description": "Article description text",
    "updatedDate": "2025-10-26T18:17:04Z",
    "createdDate": "2025-10-26T18:10:00Z"
  }
]
```

The normalization process:
- Strips HTML from descriptions
- Extracts the best available URL (from link, links array, id, or guid)
- Deduplicates entries by URL
- Converts all dates to ISO 8601 format with UTC timezone
- Preserves original metadata where possible

Logs for all tasks are available in the Airflow UI on the task-instance pages or under `logs/` in the project root.
