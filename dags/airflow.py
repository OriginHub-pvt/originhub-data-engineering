# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.providers.standard.operators.python import PythonOperator

# from src import (
#     fetch_rss_feeds,
#     store_rss_feeds,
#     normalize_feeds_to_gcs,
#     scrape_all_from_json_files,
#     filter_articles,
#     summarize_record,
#     store_summary
# )

# default_args = {
#     "owner": "OriginHub",
#     "depends_on_past": False,
#     "retries": 0,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="rss_feed_ingestion_and_normalization",
#     description="Fetch RSS feeds, store raw XML to GCS, normalize to JSON, filter relevant items, and scrapes content from the relevant items.",
#     default_args=default_args,
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=["rss", "ingestion", "normalization", "filtering", "scraping"],
#     params={
#         "rss_url": "",
#         "request_timeout": 30,
#     },
# ) as dag:
#     fetch_rss_task = PythonOperator(
#         task_id="fetch_rss_feeds",
#         python_callable=fetch_rss_feeds,
#     )

#     store_rss_task = PythonOperator(
#         task_id="store_rss_feeds",
#         python_callable=store_rss_feeds,
#         op_kwargs={"payload": fetch_rss_task.output},
#     )

#     normalize_feeds_task = PythonOperator(
#         task_id="normalize_feeds",
#         python_callable=normalize_feeds_to_gcs,
#         op_kwargs={"payload": fetch_rss_task.output},
#     )

#     filter_articles_task = PythonOperator(
#         task_id="filter_articles",
#         python_callable=filter_articles,
#         op_kwargs={"payload": normalize_feeds_task.output},
#     )

#     scrape_content_task = PythonOperator(
#         task_id="scrape_web_content",
#         python_callable=scrape_all_from_json_files,
#         op_kwargs={"payload": filter_articles_task.output},
#     )

#     fetch_rss_task >> [store_rss_task, normalize_feeds_task]
#     normalize_feeds_task >> filter_articles_task
#     filter_articles_task >> scrape_content_task

# with DAG(
#     dag_id="summarize_and_store_weaviate",
#     start_date=datetime(2024, 1, 1),
#     max_active_runs=1,
#     schedule=None,
#     params={
#         "scraped_data": {
#             "scraped_content": "This is scraped text...",
#             "scraped_metadata": {"author": "John Doe"},
#             "scraped_at": "2025-10-30T16:40:00Z",
#             "word_count": 1234,
#             "url": "https://example.com/article",
#             "title": "AI is Transforming the World"
#         }
#     },
# ) as dag:

#     summarize_task = PythonOperator(
#         task_id="summarize_record",
#         python_callable=summarize_record,
#         op_kwargs={
#             "record": {
#                 "scraped_content": "{{ params.scraped_data.scraped_content }}",
#                 "scraped_metadata": "{{ params.scraped_data.scraped_metadata }}",
#                 "scraped_at": "{{ params.scraped_data.scraped_at }}",
#                 "word_count": "{{ params.scraped_data.word_count }}",
#                 "url": "{{ params.scraped_data.url }}",
#                 "title": "{{ params.scraped_data.title }}"
#             }
#         },
#     )

#     store_task = PythonOperator(
#         task_id="store_in_weaviate",
#         python_callable=store_summary,
#         op_kwargs={"record": summarize_task.output},
#     )

#     summarize_task >> store_task

# if __name__ == "__main__":
#     dag.test()

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from src import (
    fetch_rss_feeds,
    store_rss_feeds,
    normalize_feeds_to_gcs,
    scrape_all_from_json_files,
    filter_articles,
    summarize_record,
    store_summary
)

default_args = {
    "owner": "OriginHub",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="rss_feed_ingestion_and_normalization",
    description=(
        "Fetch RSS feeds, store raw XML to GCS, normalize to JSON, "
        "filter relevant items, and scrape content from relevant items."
    ),
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["rss", "ingestion", "normalization", "filtering", "scraping"],
    params={
        "rss_url": "",
        "request_timeout": 30,
    },
) as rss_feed_dag:

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

    summarize_tasks = PythonOperator.partial(
        task_id="summarize_record",
        python_callable=summarize_record,
        max_active_tis_per_dag=2,
    ).expand(op_kwargs=scrape_content_task.output)

    store_tasks = PythonOperator.partial(
        task_id="store_in_weaviate",
        python_callable=store_summary,
    ).expand(op_kwargs=summarize_tasks.output)

    fetch_rss_task >> [store_rss_task, normalize_feeds_task]
    normalize_feeds_task >> filter_articles_task
    filter_articles_task >> scrape_content_task >> summarize_tasks >> store_tasks