from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from src import fetch_rss_feeds, store_rss_feeds, normalize_latest_feeds

default_args = {
    "owner": "Mayank",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="rss_feed_ingestion_and_normalization",
    description="Fetch provided RSS/Atom feeds and store the raw XML locally and normalize them into json.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["rss", "ingestion", "normalization"],
    params={
        "rss_url": "",
        "output_dir": "rss_data",
        "request_timeout": 30,
    },
) as dag:
    fetch_rss_task = PythonOperator(
        task_id="fetch_rss_feeds",
        python_callable=fetch_rss_feeds,
    )
    store_rss_task = PythonOperator(
        task_id="store_rss_feeds",
        python_callable=store_rss_feeds,
        op_args=[fetch_rss_task.output],
    )
    
    normalize_feeds_task = PythonOperator(
        task_id="normalize_feeds",
        python_callable=normalize_latest_feeds,
    )

    fetch_rss_task >> store_rss_task >> normalize_feeds_task

if __name__ == "__main__":
    dag.test()
