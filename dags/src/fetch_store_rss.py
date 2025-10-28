from __future__ import annotations
import os
import logging
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Tuple
from urllib.parse import urlparse
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import requests

DEFAULT_TIMEOUT = 30
XML_BUCKET_NAME = os.environ.get("XML_BUCKET_NAME")

def _normalize_urls(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        pieces = re.split(r"[,\n]", raw)
        return [piece.strip() for piece in pieces if piece.strip()]
    if isinstance(raw, Iterable):
        urls = [str(item).strip() for item in raw]
        return [url for url in urls if url]
    url = str(raw).strip()
    return [url] if url else []


def _slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    parts = [parsed.netloc] + [segment for segment in parsed.path.split("/") if segment]
    slug = "_".join(filter(None, parts)) or parsed.netloc or "feed"
    return re.sub(r"[^A-Za-z0-9_.-]", "_", slug)


def _build_filename(rss_url: str, fetched_at: datetime, extension: str = "xml") -> str:
    timestamp = fetched_at.strftime("%Y%m%d%H%M%S")
    return f"{timestamp}_{_slug_from_url(rss_url)}.{extension}"


def _extract_config(context: Any) -> Tuple[List[str], int]:
    params = context.get("params") or {}
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    urls = _normalize_urls(conf.get("rss_url", params.get("rss_url")))
    timeout = int(conf.get("request_timeout", params.get("request_timeout", DEFAULT_TIMEOUT)))
    return urls, timeout


def fetch_rss_feeds(**context: Any) -> Dict[str, Any]:
    logging.debug("Starting 'fetch_rss_feeds' with context: %s", context)
    urls, timeout = _extract_config(context)
    if not urls:
        logging.error("No RSS URL provided.")
        raise ValueError("Pass at least one RSS URL via dag_run.conf or DAG params (rss_url).")

    fetched_payloads: List[Dict[str, Any]] = []
    for rss_url in urls:
        logging.debug("Fetching RSS feed from URL: %s", rss_url)
        try:
            response = requests.get(rss_url, timeout=timeout)
            response.raise_for_status()

            fetched_at = datetime.utcnow().replace(microsecond=0)
            payload = {
                "rss_url": rss_url,
                "content": response.text,
                "fetched_at": fetched_at.isoformat(),
            }
            
            logging.info("Fetched %s (%d bytes)", rss_url, len(response.text))
            fetched_payloads.append(payload)
        except requests.exceptions.RequestException as e:
            logging.error("Failed to fetch RSS feed from %s: %s", rss_url, e)
            raise Exception(e)

    logging.debug("Fetch complete. Payloads: %s", fetched_payloads)
    return {"feeds": fetched_payloads}

def store_rss_feeds(
    payload: Dict[str, Any],
    prefix: str = "",
    gcp_conn_id: str = "google_cloud_default",
    gzip: bool = False,
) -> List[str]:
    feeds = payload.get("feeds") or []
    if not feeds:
        raise ValueError("No feed payloads supplied to store.")

    if prefix and not prefix.endswith("/"):
        prefix += "/"

    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    saved: List[str] = []

    for feed in feeds:
        rss_url = feed["rss_url"]
        fetched_at_str = feed.get("fetched_at")
        try:
            fetched_at = (
                datetime.fromisoformat(fetched_at_str.replace("Z", "+00:00"))
                if fetched_at_str else datetime.utcnow()
            )
        except Exception:
            logging.warning("Bad fetched_at '%s' for %s; using now()", fetched_at_str, rss_url)
            fetched_at = datetime.utcnow()

        filename = _build_filename(rss_url, fetched_at, extension="xml")
        object_name = f"{prefix}{filename}" if prefix else filename
        hook.upload(
            bucket_name=XML_BUCKET_NAME,
            object_name=object_name,
            data=feed["content"],
            mime_type="application/xml",
            gzip=gzip,
        )
        saved.append(f"gs://{XML_BUCKET_NAME}/{object_name}")

    logging.info("Stored %d feeds to GCS", len(saved))
    return saved
