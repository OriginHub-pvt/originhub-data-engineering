from __future__ import annotations

import os
import json
import logging
from typing import Any, Dict, List

from dotenv import load_dotenv
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

load_dotenv()

MODEL_BUCKET = os.environ.get("MODEL_BUCKET", "origin-hub_model-weights")
MODEL_NAME = os.environ.get("MODEL_NAME", "slm_filter")
MODEL_VERSION = os.environ.get("MODEL_VERSION", "1")

# This matches your existing Airflow connection (already defined via env)
GCP_CONN_ID = "google_cloud_default"

# Temporary local cache for model weights
MODEL_TMP_PATH = f"/tmp/{MODEL_NAME}_v{MODEL_VERSION}"

# Logging level from env (INFO by default)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

def load_model_from_gcs() -> tuple:
    """
    Download and load tokenizer and model from GCS.
    If already cached locally in /tmp, it skips downloading.
    """
    if not os.path.exists(MODEL_TMP_PATH):
        os.makedirs(MODEL_TMP_PATH, exist_ok=True)
        logger.info(f"Downloading model {MODEL_NAME} (v{MODEL_VERSION}) from gs://{MODEL_BUCKET}")

        try:
            hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
            blobs = hook.list(
                bucket_name=MODEL_BUCKET,
                prefix=f"{MODEL_NAME}/v{MODEL_VERSION}/"
            )

            if not blobs:
                raise FileNotFoundError(
                    f"No model found at gs://{MODEL_BUCKET}/{MODEL_NAME}/v{MODEL_VERSION}/"
                )

            for blob in blobs:
                local_file = os.path.join(MODEL_TMP_PATH, os.path.basename(blob))
                hook.download(bucket_name=MODEL_BUCKET, object_name=blob, filename=local_file)
                logger.debug(f"Downloaded {blob} -> {local_file}")

        except Exception as e:
            logger.error(f"Failed to download model from GCS: {e}")
            raise

    logger.info(f"Loading model and tokenizer from {MODEL_TMP_PATH}")
    try:
        tokenizer = AutoTokenizer.from_pretrained("distilbert-base-uncased")
        model = AutoModelForSequenceClassification.from_pretrained(MODEL_TMP_PATH)
        model.eval()
    except Exception as e:
        logger.error(f"Failed to load tokenizer/model: {e}")
        raise

    logger.info("Model and tokenizer loaded successfully.")
    return tokenizer, model

def predict_relevance(
    items: List[Dict[str, Any]],
    tokenizer: AutoTokenizer,
    model: AutoModelForSequenceClassification
) -> List[Dict[str, Any]]:
    """
    Run model inference on (title + description) for each article.
    Returns only those with label == 1.
    """
    filtered_items = []

    for item in items:
        text = f"{item.get('title', '').strip()} {item.get('description', '').strip()}"
        if not text.strip():
            continue

        try:
            inputs = tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                padding=True,
                max_length=256
            )

            with torch.no_grad():
                logits = model(**inputs).logits
                pred = torch.argmax(logits, dim=-1).item()

            if pred == 1:
                filtered_items.append(item)

        except Exception as e:
            logger.warning(f"Prediction failed for item '{item.get('title', '')}': {e}")

    logger.info(f"Filtered {len(filtered_items)} of {len(items)} items as relevant.")
    return filtered_items


def filter_articles(**context: Any) -> List[Dict[str, Any]]:
    """
    Airflow PythonOperator callable.
    
    Input (XCom from normalize_feeds):
    {
        "rss_url": "...",
        "fetched_at": "...",
        "item_count": N,
        "items": [ {...}, {...} ]
    }

    Output (returned to XCom automatically):
    [ {...only relevant items...} ]
    """
    try:
        payload = context.get("payload") or context["ti"].xcom_pull(task_ids="normalize_feeds")
        if not payload:
            raise ValueError("No payload found for filtering step.")

        logger.info("Starting filter_articles step with model v%s", MODEL_VERSION)
        tokenizer, model = load_model_from_gcs()

        logger.info(f"Received {payload} items to filter.")
        items = payload.get("items", [])
        filtered_items = predict_relevance(items, tokenizer, model)

        logger.info(f"Filtering completed — returning {len(filtered_items)} relevant items.")
        return filtered_items

    except Exception as e:
        logger.error(f"Error in filter_articles: {e}")
        raise
