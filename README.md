# RSS Feed Ingestion DAG

This project ships an Apache Airflow DAG named `rss_feed_ingestion` that fetches one or more RSS/Atom feeds and stores the raw XML to a local folder. The DAG is tagged with `rss`, so you can quickly filter for it inside the Airflow UI.

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

Use the search box or the `rss` tag to find the `rss_feed_ingestion` DAG. Unpause it if needed.

## Trigger the DAG

You can trigger from the UI (`Trigger DAG w/ config`) or the CLI. The DAG accepts the following optional parameters:

- `rss_url`: list of feed URLs (string, comma/newline separated list, or JSON array)
- `output_dir`: target directory relative to `/opt/airflow/dags` (defaults to `rss_data`)
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
  airflow dags trigger rss_feed_ingestion \
  --conf '{"rss_url": "https://feeds.arstechnica.com/arstechnica/index/"}'
```

## Output

Fetched feeds are stored under `dags/rss_data/` (or the directory you override with `output_dir`). Each file is timestamped and suffixed with a slug derived from the feed URL, for example:

```
./dags/rss_data/20251026002601_feeds.arstechnica.com_arstechnica_index.xml
```

Logs for the fetch and store tasks are available in the Airflow UI on the task-instance pages or under `logs/` in the project root.
