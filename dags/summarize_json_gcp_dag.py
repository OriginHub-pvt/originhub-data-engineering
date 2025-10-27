from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from src.summarizer_functions import summarize_record, serialize_json
import json


with DAG(
    dag_id="summarize_json_gcp_dag",
    start_date=datetime(2025, 10, 26),
    schedule_interval=None,
    catchup=False,
    tags=["gcp", "summarization", "json"],
    params={
        "source_bucket": "input-bucket-name",
        "source_blob": "path/to/input.json",
        "dest_bucket": "output-bucket-name",
        "dest_blob": "path/to/summarized_output.json",
    },
) as dag:

    @task()
    def download_json_from_gcs(params=None, dag_run=None):
        """Download JSON from GCS."""
        conf = (dag_run.conf if dag_run else {}) or {}
        bucket = conf.get("source_bucket") or params["source_bucket"]
        blob = conf.get("source_blob") or params["source_blob"]

        hook = GCSHook(gcp_conn_id="gcp_default")
        content = hook.download(bucket_name=bucket, object_name=blob)
        data = json.loads(content.decode("utf-8"))
        print(f" Downloaded {len(data)} records from gs://{bucket}/{blob}")
        return data

    @task()
    def summarize_each_record(record: dict):
        """Summarize each record (runs in parallel using task mapping)."""
        return summarize_record(record)

    @task()
    def upload_json_to_gcs(records, params=None, dag_run=None):
        """Upload summarized JSON to destination bucket."""
        conf = (dag_run.conf if dag_run else {}) or {}
        bucket = conf.get("dest_bucket") or params["dest_bucket"]
        blob = conf.get("dest_blob") or params["dest_blob"]

        hook = GCSHook(gcp_conn_id="gcp_default")
        file_bytes = serialize_json(records)
        hook.upload(
            bucket_name=bucket,
            object_name=blob,
            data=file_bytes.getvalue(),
            mime_type="application/json"
        )
        print(f" Uploaded summarized JSON to gs://{bucket}/{blob}")

    # DAG Flow
    json_list = download_json_from_gcs()
    summarized_list = summarize_each_record.expand(record=json_list)
    upload_json_to_gcs(summarized_list)
