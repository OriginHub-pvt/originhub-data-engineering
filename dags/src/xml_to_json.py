# -*- coding: utf-8 -*-
"""
Normalize RSS/Atom XML files from ./dags/rss_data into unified JSON schema:
{
    "title": "",
    "url": "",
    "description": "",
    "updatedDate": "",
    "createdDate": ""
}
"""

import os
import glob
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import feedparser
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re

from dotenv import load_dotenv

load_dotenv()

JSON_BUCKET_NAME = os.environ.get("JSON_BUCKET_NAME")

# Setup logging
def setup_logging():
    logging.basicConfig(level=logging.DEBUG, force=True)

def _build_filename(rss_url: str, fetched_at: datetime, extension: str = "xml") -> str:
    timestamp = fetched_at.strftime("%Y%m%d%H%M%S")
    return f"{timestamp}_{_slug_from_url(rss_url)}.{extension}"

def _slug_from_url(url: str) -> str:
    parsed = urlparse(url)
    parts = [parsed.netloc] + [segment for segment in parsed.path.split("/") if segment]
    slug = "_".join(filter(None, parts)) or parsed.netloc or "feed"
    return re.sub(r"[^A-Za-z0-9_.-]", "_", slug)

# ---------- utility functions ----------
def _to_iso_from_struct(t) -> Optional[str]:
    if not t:
        return None
    try:
        dt = datetime(t.tm_year, t.tm_mon, t.tm_mday,
                    t.tm_hour, t.tm_min, t.tm_sec, tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _strip_html(s: Optional[str]) -> str:
    if not s:
        return ""
    text = BeautifulSoup(s, "html.parser").get_text(separator=" ", strip=True)
    # Collapse multiple spaces into single spaces
    import re
    return re.sub(r'\s+', ' ', text)


def _first_url_from_links(links: Any) -> Optional[str]:
    if not links:
        return None
    for l in links:
        if (l.get("rel") or "alternate") == "alternate" and l.get("href"):
            return l["href"].strip()
    for l in links:
        if l.get("href"):
            return l["href"].strip()
    return None


def _best_url(entry: Dict[str, Any]) -> Optional[str]:
    url = (entry.get("link") or "").strip() if entry.get("link") else None
    if url:
        return url
    url = _first_url_from_links(entry.get("links"))
    if url:
        return url
    _id = entry.get("id")
    if isinstance(_id, str) and _id.startswith(("http://", "https://")):
        return _id.strip()
    guid = entry.get("guid")
    if isinstance(guid, str) and guid.startswith(("http://", "https://")):
        return guid.strip()
    return None


def _best_created(entry: Dict[str, Any]) -> Optional[str]:
    return (_to_iso_from_struct(entry.get("published_parsed"))
            or _to_iso_from_struct(entry.get("created_parsed"))
            or _to_iso_from_struct(entry.get("updated_parsed")))


def _best_updated(entry: Dict[str, Any], created_iso: Optional[str]) -> Optional[str]:
    return _to_iso_from_struct(entry.get("updated_parsed")) or created_iso


def _coalesce_description(entry: Dict[str, Any]) -> str:
    # Check if summary exists (even if empty) - priority field
    if "summary" in entry:
        return _strip_html(entry["summary"] or "")
    
    # Fall back to description
    tx = entry.get("description")
    if tx:
        return _strip_html(tx)
    
    # Fall back to content array
    content = entry.get("content") or []
    if content and isinstance(content, list):
        for item in content:
            if item.get("value"):
                return _strip_html(item["value"])
    
    # Check alternative content tags
    tx = entry.get("content:encoded", "")
    return _strip_html(tx)


def normalize_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    title = (entry.get("title") or "").strip()
    url = _best_url(entry) or ""
    description = _coalesce_description(entry)
    created_iso = _best_created(entry)
    updated_iso = _best_updated(entry, created_iso)
    return {
        "title": title,
        "url": url,
        "description": description,
        "updatedDate": updated_iso or "",
        "createdDate": created_iso or "",
    }


def parse_xml_content(content: Any) -> List[Dict[str, Any]]:
    feed = feedparser.parse(content)
    return [normalize_entry(e) for e in feed.entries]


def parse_xml_file(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, "rb") as f:
        data = f.read()
    return parse_xml_content(data)


# ---------- directory loop ----------
def normalize_all_feeds(input_dir: str = "./dags/rss_data", output_dir: str = "./dags/normalized_feeds") -> List[str]:
    """
    Normalize all XML feeds and save individual JSON files.
    Returns list of output file paths.
    """
    xml_files = sorted(glob.glob(os.path.join(input_dir, "*.xml")))
    output_files = []

    if not xml_files:
        print(f"[warn] No XML files found in {input_dir}")
        return []

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    for xml_path in xml_files:
        try:
            parsed = parse_xml_file(xml_path)
            deduped = dedupe_by_url(parsed)
            
            # Get the base name of the XML file
            xml_basename = os.path.basename(xml_path)
            json_filename = os.path.splitext(xml_basename)[0] + ".normalized.json"
            json_path = os.path.join(output_dir, json_filename)
            
            # Write the normalized data to JSON file
            with open(json_path, "w", encoding="utf-8") as w:
                json.dump(deduped, w, ensure_ascii=False, indent=2)
            
            output_files.append(json_path)
            print(f"[info] Normalized {len(deduped)} items written to {json_path}")
            
        except feedparser.FeedParserError as fp_ex:
            logging.error(f"[error] FeedParserError parsing {xml_path}: {fp_ex}")
        except Exception as ex:
            logging.error(f"[error] Exception parsing {xml_path}: {ex}")

    return output_files


def dedupe_by_url(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set[Any]()
    deduped = []
    for it in items:
        key = it.get("url") or (it.get("title", "").lower(), it.get("createdDate", ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(it)
    return deduped


def normalize_feeds_to_gcs(
    payload: Dict[str, Any],
    prefix: str = "",
    gcp_conn_id: str = "google_cloud_default",
    gzip: bool = False,
) -> List[str]:
    feeds = payload.get("feeds") or []
    if not feeds:
        raise ValueError("No feed payloads supplied to normalize.")

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

        items = parse_xml_content(feed["content"])
        deduped = dedupe_by_url(items)

        json_payload = {
            "rss_url": rss_url,
            "fetched_at": fetched_at_str,
            "item_count": len(deduped),
            "items": deduped,
        }

        filename = _build_filename(rss_url, fetched_at, extension="json")
        object_name = f"{prefix}{filename}" if prefix else filename

        hook.upload(
            bucket_name=JSON_BUCKET_NAME,
            object_name=object_name,
            data=json.dumps(json_payload, ensure_ascii=False, indent=2),
            mime_type="application/json",
            gzip=gzip,
        )
        saved.append(f"gs://{JSON_BUCKET_NAME}/{object_name}")

    logging.info("Normalized %d feeds to JSON GCS objects", len(saved))
    return saved


def normalize_latest_feeds(**context) -> List[str]:
    """Retained for local workflows and existing tests.

    Processes XML files under the DAG directory and writes normalized JSON files locally.
    """
    dags_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_dir = os.path.join(dags_dir, "rss_data")
    output_dir = os.path.join(dags_dir, "normalized_feeds")

    logging.info("Normalizing feeds from %s to %s", input_dir, output_dir)
    return normalize_all_feeds(input_dir, output_dir)


if __name__ == "__main__":
    # Initialize logging
    setup_logging()

    output_files = normalize_all_feeds()
    
    print(f"[info] Processed {len(output_files)} XML files and created {len(output_files)} normalized JSON files")
