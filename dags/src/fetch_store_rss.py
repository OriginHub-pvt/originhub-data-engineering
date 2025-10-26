from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from urllib.parse import urlparse

import requests

DEFAULT_TIMEOUT = 30


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


def _build_filename(rss_url: str, fetched_at: datetime) -> str:
    timestamp = fetched_at.strftime("%Y%m%d%H%M%S")
    return f"{timestamp}_{_slug_from_url(rss_url)}.xml"


def _resolve_output_dir(output_dir: str | None) -> Path:
    base_dir = Path(output_dir or "rss_data")
    if not base_dir.is_absolute():
        dags_folder = Path(__file__).resolve().parents[1]
        base_dir = dags_folder / base_dir
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir


def _extract_config(context: Any) -> Tuple[List[str], str | None, int]:
    params = context.get("params") or {}
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}

    urls = _normalize_urls(conf.get("rss_url", params.get("rss_url")))
    output_dir = conf.get("output_dir", params.get("output_dir"))
    timeout = int(conf.get("request_timeout", params.get("request_timeout", DEFAULT_TIMEOUT)))
    return urls, output_dir, timeout


def fetch_rss_feeds(**context: Any) -> Dict[str, Any]:
    logging.debug("Starting 'fetch_rss_feeds' with context: %s", context)
    urls, output_dir, timeout = _extract_config(context)
    if not urls:
        logging.error("No RSS URL provided.")
        raise ValueError("Pass at least one RSS URL via dag_run.conf or DAG params (rss_url).")

    fetched_payloads: List[Dict[str, Any]] = []
    for rss_url in urls:
        try:
            logging.debug("Fetching RSS feed from URL: %s", rss_url)
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
            continue

    logging.debug("Fetch complete. Payloads: %s", fetched_payloads)
    return {
        "feeds": fetched_payloads,
        "output_dir": output_dir,
    }


def store_rss_feeds(payload: Dict[str, Any]) -> List[str]:
    logging.debug("Starting 'store_rss_feeds' with payload: %s", payload)
    feeds = payload.get("feeds") or []
    if not feeds:
        logging.error("No feed payloads supplied to store.")
        raise ValueError("No feed payloads supplied to store.")

    output_dir = payload.get("output_dir")
    base_dir = _resolve_output_dir(output_dir)
    logging.debug("Resolved output directory: %s", base_dir)

    saved_paths: List[str] = []
    for feed in feeds:
        rss_url = feed["rss_url"]
        fetched_at_str = feed.get("fetched_at")
        fetched_at = datetime.fromisoformat(fetched_at_str) if fetched_at_str else datetime.utcnow()

        filename = _build_filename(rss_url, fetched_at)
        destination = base_dir / filename
        logging.debug("Saving RSS feed to %s", destination)

        try:
            destination.write_text(feed["content"], encoding="utf-8")
            logging.info("Saved RSS feed from %s to %s", rss_url, destination)
            saved_paths.append(str(destination))
        except IOError as e:
            logging.error("Failed to save RSS feed from %s to %s: %s", rss_url, destination, e)
            continue

    logging.debug("Store complete. Saved paths: %s", saved_paths)
    return saved_paths
