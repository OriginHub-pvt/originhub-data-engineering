from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from src import (
    fetch_rss_feeds,
    store_rss_feeds,
    normalize_feeds_to_gcs,
    scrape_all_from_json_files,
    filter_articles,
    summarize_record,
    store_in_weaviate
)

default_args = {
    "owner": "OriginHub",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="rss_feed_ingestion_and_normalization",
    description="Fetch RSS feeds, store raw XML to GCS, normalize to JSON, filter relevant items, and scrapes content from the relevant items.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["rss", "ingestion", "normalization", "filtering", "scraping"],
    params={
        "rss_url": "",
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
        op_kwargs={"payload": fetch_rss_task.output},
    )

    normalize_feeds_task = PythonOperator(
        task_id="normalize_feeds",
        python_callable=normalize_feeds_to_gcs,
        op_kwargs={"payload": fetch_rss_task.output},
    )

    filter_articles_task = PythonOperator(
        task_id="filter_articles",
        python_callable=filter_articles,
        op_kwargs={"payload": normalize_feeds_task.output},
    )

    scrape_content_task = PythonOperator(
        task_id="scrape_web_content",
        python_callable=scrape_all_from_json_files,
        op_kwargs={"payload": filter_articles_task.output},
    )

    def trigger_summary_dags(**context):
        """Trigger the summarize_and_store_weaviate DAG once for each article."""
        articles = context["ti"].xcom_pull(task_ids="scrape_web_content")

        if not articles:
            print("No articles found to summarize.")
            return

        for idx, article in enumerate(articles):
            trigger = TriggerDagRunOperator(
                task_id=f"trigger_summary_dag_{idx}",
                trigger_dag_id="summarize_and_store_weaviate",
                conf={"article": article},
                wait_for_completion=False,
            )
            trigger.execute(context)
            print(f"Triggered summarization DAG for article #{idx+1}")

    trigger_summary_task = PythonOperator(
        task_id="trigger_summary_dags",
        python_callable=trigger_summary_dags,
        provide_context=True,
    )

    fetch_rss_task >> [store_rss_task, normalize_feeds_task]
    normalize_feeds_task >> filter_articles_task
    filter_articles_task >> scrape_content_task
    scrape_content_task >> trigger_summary_task

with DAG(
    dag_id="summarize_and_store_weaviate",
    description="Summarize article content and store the summarized record into Weaviate.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["summarization", "weaviate", "embedding", "originhub"],
    params={
        "article": {},
    },
) as summarize_and_store_dag:

    summarize_task = PythonOperator(
        task_id="summarize_record",
        python_callable=summarize_record,
        op_kwargs={"record": "{{ params.article }}"},
    )  

    store_task = PythonOperator(
        task_id="store_in_weaviate",
        python_callable=store_in_weaviate,
        op_kwargs={"record": summarize_task.output},
    )

    summarize_task >> store_task

if __name__ == "__main__":
    dag.test()
    