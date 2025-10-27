# RSS Feed Ingestion and Normalization DAG

This project ships an Apache Airflow DAG named `rss_feed_ingestion_and_normalization` that fetches one or more RSS/Atom feeds, stores the raw XML in Google Cloud Storage, and normalizes them into a unified JSON schema that is also stored in GCS. The DAG is tagged with `rss`, `ingestion`, and `normalization`, so you can quickly filter for it inside the Airflow UI.

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

## Create Connection to GCP

1) Go to the web UI -> Admin -> Connections -> Add Connection
2) ID: google_cloud_default
3) Connection Type: Google Cloud
4) In the Extra Fields -> Keyfile JSON, copy the whole JSON of the service account
5) Save the Connection

## DAG Workflow

The DAG consists of three tasks with a fan-out after the fetch step:
1. **fetch_rss_feeds**: Fetches RSS/Atom feed content from the provided URLs and shares it via XCom
2. **store_rss_feeds**: Persists the raw XML feed content to the `xml-temp-storage` GCS bucket (prefix `rss/xml/` by default)
3. **normalize_feeds**: Normalizes the fetched XML into JSON using a shared schema and uploads it to the `json-train-storage` GCS bucket (prefix `rss/json/` by default)

## Trigger the DAG

You can trigger from the UI (`Trigger DAG w/ config`) or the CLI. The DAG accepts the following optional parameters:

- `rss_url`: list of feed URLs (string, comma/newline separated list, or JSON array)
- `request_timeout`: HTTP timeout in seconds (default `30`)

Example JSON config:

```json
{
  "rss_url": "https://feeds.arstechnica.com/arstechnica/index/"
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

### GCS Outputs

- Raw XML feeds are written to `gs://xml-temp-storage/rss/xml/<timestamped_slug>.xml`
- Normalized JSON feeds are written to `gs://json-train-storage/rss/json/<timestamped_slug>.json`

Each JSON object contains:

```json
{
  "rss_url": "https://example.com/feed",
  "fetched_at": "2025-10-26T18:10:00Z",
  "item_count": 42,
  "items": [
    {
      "title": "Article Title",
      "url": "https://example.com/article",
      "description": "Plain-text synopsis",
      "updatedDate": "2025-10-26T18:17:04Z",
      "createdDate": "2025-10-26T18:10:00Z"
    }
  ]
}
```

The normalization process:
- Strips HTML from descriptions
- Extracts the best available URL (from link, links array, id, or guid)
- Deduplicates entries by URL
- Converts all dates to ISO 8601 format with UTC timezone
- Preserves original metadata where possible

Logs for all tasks are available in the Airflow UI on the task-instance pages or under `logs/` in the project root.
