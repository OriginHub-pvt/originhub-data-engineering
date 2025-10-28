from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from src import fetch_rss_feeds, store_rss_feeds, normalize_feeds_to_gcs, scrape_all_from_json_files

default_args = {
    "owner": "OriginHub",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="rss_feed_ingestion_and_normalization",
    description="Fetch feeds, store raw XML to GCS, normalize to JSON.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["rss", "ingestion", "normalization"],
    params={
        "rss_url": "",
        "request_timeout": 30,
    },
) as dag:
    fetch_rss_task = PythonOperator(
        task_id="fetch_rss_feeds",
        python_callable=fetch_rss_feeds,  # MUST return {"feeds": [{rss_url, content, fetched_at?}, ...]}
    )

    store_rss_task = PythonOperator(
        task_id="store_rss_feeds",
        python_callable=store_rss_feeds,
        op_kwargs={"payload": fetch_rss_task.output},
    )

    normalize_feeds_task = PythonOperator(
        task_id="normalize_feeds",
        python_callable=normalize_feeds_to_gcs,
        op_kwargs={"payload": fetch_rss_task.output},
    )

    fetch_rss_task >> [store_rss_task, normalize_feeds_task]


# Web Content Scraping DAG
with DAG(
    dag_id="web_content_scraping",
    description="Scrape web content from URLs in filtered_data JSON files",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["scraping", "web-content", "data-enrichment"],
    params={
        "json_dir": "dags/filtered_data",
    },
) as scraping_dag:
    scrape_content_task = PythonOperator(
        task_id="scrape_web_content",
        python_callable=scrape_all_from_json_files,
        op_kwargs={
            "json_dir": "dags/filtered_data"
        },
    )


if __name__ == "__main__":
    dag.test()
