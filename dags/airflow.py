from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from dags.src import fetch_rss_feeds, store_rss_feeds

default_args = {
    "owner": "Mayank",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="rss_feed_ingestion",
    description="Fetch provided RSS/Atom feeds and store the raw XML locally.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["rss", "ingestion"],
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

    fetch_rss_task >> store_rss_task

if __name__ == "__main__":
    dag.test()
