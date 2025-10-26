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
import feedparser
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import logging

# Setup logging
def setup_logging():
    logging.basicConfig(level=logging.DEBUG)


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
    return BeautifulSoup(s, "html.parser").get_text(separator=" ", strip=True)


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
    tx = entry.get("summary") or entry.get("description")
    if not tx:
        content = entry.get("content") or []
        if content and isinstance(content, list):
            for item in content:
                if item.get("value"):
                    tx = item["value"]
                    break

    # Check alternative content tags
    if not tx:
        tx = entry.get("content:encoded", "")
    
    return _strip_html(tx or "")


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


def parse_xml_file(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, "rb") as f:
        data = f.read()
    feed = feedparser.parse(data)
    return [normalize_entry(e) for e in feed.entries]


# ---------- directory loop ----------
def normalize_all_feeds(input_dir: str = "./dags/rss_data") -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    xml_files = sorted(glob.glob(os.path.join(input_dir, "*.xml")))

    if not xml_files:
        print(f"[warn] No XML files found in {input_dir}")
        return []

    for path in xml_files:
        try:
            parsed = parse_xml_file(path)
            items.extend(parsed)
        except feedparser.FeedParserError as fp_ex:
            logging.error(f"[error] FeedParserError parsing {path}: {fp_ex}")
        except Exception as ex:
            logging.error(f"[error] Exception parsing {path}: {ex}")

    return items


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


if __name__ == "__main__":
    output_path = "./dags/normalized_feeds.json"

    # Initialize logging
    setup_logging()

    feeds = normalize_all_feeds()
    feeds = dedupe_by_url(feeds)

    with open(output_path, "w", encoding="utf-8") as w:
        json.dump(feeds, w, ensure_ascii=False, indent=2)

    print(f"[info] Normalized {len(feeds)} items written to {output_path}")