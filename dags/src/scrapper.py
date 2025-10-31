# -*- coding: utf-8 -*-
"""
General web scraper for extracting content from any URL.
Supports HTML parsing and structured data extraction.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from datetime import datetime, UTC, timezone

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)

# Airflow API base URL (for local Airflow instance)
airflow_base_url = os.getenv("AIRFLOW_API_BASE_URL", "http://localhost:8080")
dag_id = "summarize_and_store_weaviate"
api_endpoint = f"{airflow_base_url}/api/v2/dags/{dag_id}/dagRuns"

# Optional authentication (if Airflow UI login is enabled)
airflow_user = os.getenv("AIRFLOW_USERNAME", "airflow")
airflow_password = os.getenv("AIRFLOW_PASSWORD", "airflow")

class WebScraper:
    """
    A flexible web scraper for extracting content from URLs.
    """
    
    def __init__(self, timeout: int = 30, headers: Optional[Dict[str, str]] = None):
        """
        Initialize the web scraper.
        
        Args:
            timeout: Request timeout in seconds
            headers: Custom HTTP headers to send with requests
        """
        self.timeout = timeout
        self.headers = headers or {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
    
    def fetch_url(self, url: str, verify_ssl: bool = True) -> Dict[str, Any]:
        """
        Fetch content from a URL.
        
        Args:
            url: The URL to scrape
            verify_ssl: Whether to verify SSL certificates
            
        Returns:
            Dictionary containing the fetched data
        """
        logging.info(f"Fetching URL: {url}")
        
        try:
            response = requests.get(url, timeout=self.timeout, headers=self.headers, verify=verify_ssl)
            response.raise_for_status()
            
            fetched_at = datetime.now(UTC)
            
            return {
                "url": url,
                "status_code": response.status_code,
                "content": response.text,
                "content_length": len(response.content),
                "content_type": response.headers.get('Content-Type', ''),
                "fetched_at": fetched_at.isoformat(),
                "headers": dict(response.headers)
            }
            
        except requests.exceptions.SSLError as e:
            # Try without SSL verification if SSL fails
            if verify_ssl:
                logging.warning(f"SSL verification failed, retrying without verification...")
                return self.fetch_url(url, verify_ssl=False)
            raise
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch URL {url}: {e}")
            raise
    
    def parse_html(self, html_content: str) -> BeautifulSoup:
        """
        Parse HTML content using BeautifulSoup.
        
        Args:
            html_content: HTML string to parse
            
        Returns:
            BeautifulSoup object
        """
        return BeautifulSoup(html_content, 'html.parser')
    
    def extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract metadata from HTML.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Dictionary containing metadata
        """
        metadata = {}
        
        # Extract title
        title_tag = soup.find('title')
        metadata['title'] = title_tag.text.strip() if title_tag else ''
        
        # Extract meta description
        desc_tag = soup.find('meta', attrs={'name': 'description'})
        metadata['description'] = desc_tag.get('content', '') if desc_tag else ''
        
        # Extract all meta tags
        metadata['meta_tags'] = {}
        for meta in soup.find_all('meta'):
            name = meta.get('name') or meta.get('property', '')
            content = meta.get('content', '')
            if name and content:
                metadata['meta_tags'][name] = content
        
        # Extract Open Graph tags
        og_tags = {}
        for meta in soup.find_all('meta', property=lambda x: x and x.startswith('og:')):
            og_tags[meta.get('property', '')] = meta.get('content', '')
        metadata['og_tags'] = og_tags
        
        return metadata
    
    def extract_text_content(self, soup: BeautifulSoup) -> str:
        """
        Extract clean text content from HTML.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Plain text content
        """
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Get text
        text = soup.get_text()
        
        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text
    
    def extract_links(self, soup: BeautifulSoup, base_url: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Extract all links from the page.
        
        Args:
            soup: BeautifulSoup object
            base_url: Base URL for resolving relative links
            
        Returns:
            List of link dictionaries
        """
        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            text = a_tag.text.strip()
            
            if base_url and not href.startswith(('http://', 'https://')):
                from urllib.parse import urljoin
                href = urljoin(base_url, href)
            
            links.append({
                'url': href,
                'text': text
            })
        
        return links
    
    def extract_images(self, soup: BeautifulSoup, base_url: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Extract all images from the page.
        
        Args:
            soup: BeautifulSoup object
            base_url: Base URL for resolving relative URLs
            
        Returns:
            List of image dictionaries
        """
        images = []
        for img_tag in soup.find_all('img'):
            src = img_tag.get('src', '')
            alt = img_tag.get('alt', '')
            
            if base_url and src and not src.startswith(('http://', 'https://')):
                from urllib.parse import urljoin
                src = urljoin(base_url, src)
            
            if src:
                images.append({
                    'url': src,
                    'alt': alt
                })
        
        return images
    
    def scrape_full(self, url: str) -> Dict[str, Any]:
        """
        Perform a full scrape of a URL, extracting all available data.
        
        Args:
            url: The URL to scrape
            
        Returns:
            Dictionary containing all scraped data
        """
        # Fetch the content
        fetch_result = self.fetch_url(url)
        
        # Parse HTML
        soup = self.parse_html(fetch_result['content'])
        
        # Extract various data
        result = {
            "url": url,
            "fetched_at": fetch_result['fetched_at'],
            "metadata": self.extract_metadata(soup),
            "text_content": self.extract_text_content(soup),
            "links": self.extract_links(soup, url),
            "images": self.extract_images(soup, url),
            "word_count": len(self.extract_text_content(soup).split()),
        }
        
        return result


# Convenience function for quick scraping
def scrape_url(url: str, timeout: int = 30) -> Dict[str, Any]:
    """
    Quick function to scrape a URL.
    
    Args:
        url: The URL to scrape
        timeout: Request timeout in seconds
        
    Returns:
        Dictionary containing scraped data
    """
    scraper = WebScraper(timeout=timeout)
    return scraper.scrape_full(url)


# Airflow task function
def scrape_web_content(**context: Any) -> Dict[str, Any]:
    """
    Airflow task to scrape web content from URLs.
    
    Usage:
        Trigger with dag_run.conf: {"url": "https://example.com"}
        Or set DAG params: {"url": "https://example.com"}
    """
    # Extract configuration
    params = context.get("params") or {}
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    
    url = conf.get("url") or params.get("url")
    timeout = int(conf.get("timeout", params.get("timeout", 30)))
    
    if not url:
        raise ValueError("URL must be provided via dag_run.conf or DAG params")
    
    logging.info(f"Scraping URL: {url}")
    
    scraper = WebScraper(timeout=timeout)
    result = scraper.scrape_full(url)
    
    logging.info(f"Successfully scraped {url}")
    return result


def scrape_all_from_json_files(**context: Any) -> Dict[str, Any]:
    """
    Scrape all URLs from filtered JSON feed items and automatically trigger summarization DAGs
    using the Airflow REST API (works for Airflow 3.x hosted on localhost:8080).

    Args:
        context: Airflow task context, containing XCom or upstream task outputs

    Returns:
        Dictionary containing basic summary metrics (triggered and failed counts)
    """
    scraper = WebScraper()

    # Retrieve feed items from upstream task (filter_articles)
    feed_items = context.get("payload") or context["ti"].xcom_pull(task_ids="filter_articles")
    total_feeds = len(feed_items) if feed_items else 0

    logging.info(f"Found {total_feeds} feeds to scrape and summarize.")

    triggered = 0
    failed = 0

    for idx, item in enumerate(feed_items or [], 1):
        url = item.get("url")
        title = item.get("title", f"Article {idx}")

        if not url:
            logging.warning(f"[{idx}/{total_feeds}] Skipping item with no URL ({title}).")
            failed += 1
            continue

        result={**item}

        try:
            # --- Step 1: Scrape the URL content ---
            logging.info(f"Scraping [{idx}/{total_feeds}]: {url}")
            scraped_data = scraper.scrape_full(url)

            # --- Step 2: Merge scraped data with original feed metadata ---
            result = {
                **item,
                "scraped_content": scraped_data.get("text_content", ""),
                "scraped_metadata": scraped_data.get("metadata", {}),
                "scraped_at": scraped_data.get("fetched_at", ""),
                "word_count": scraped_data.get("word_count", 0),
            }
            logging.info({result})
        except requests.exceptions.RequestException as api_err:
            logging.error(f"API error while triggering {dag_id} for {url}: {api_err}")
            failed += 1
        except Exception as e:
            logging.error(f"✗ Failed to scrape or trigger for {url}: {e}")
            failed += 1
        finally:
            logging.info("Finally block reached.")
            # --- Step 3: Trigger summarization DAG via REST API ---
            token = generate_JWT()
            logical_date = datetime.now(timezone.utc).isoformat()
            response = requests.post(
                api_endpoint,
                json={"logical_date": logical_date,
                      "conf": {"scraped_data": result}},
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {token}"
                },
            )
            response.raise_for_status()

            logging.info(f"Triggered {dag_id} for '{title}' successfully")
            triggered += 1

    logging.info(f"Completed scraping: {triggered} triggered, {failed} failed.")
    return {"triggered": triggered, "failed": failed}

def generate_JWT():
    """
    Generate a JWT token for Airflow API authentication.
    Returns:
        A JWT token string
    """
    endpoint_url = f"{airflow_base_url}/auth/token"
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "username": airflow_user,
        "password": airflow_password
    }

    response = requests.post(endpoint_url, headers=headers, json=data)
    logging.info(f"Generated JWT token for Airflow API authentication. {json.loads(response.text)}")
    return json.loads(response.text)["access_token"]

if __name__ == "__main__":
    # Process all JSON files in filtered_data directory
    results = scrape_all_from_json_files("dags/filtered_data")
    
    if results:
        # Create scraped_data directory if it doesn't exist
        output_dir = "dags/scraped_data"
        os.makedirs(output_dir, exist_ok=True)
        
        # Save title and scraped_content from each result
        output_file = os.path.join(output_dir, "scraped_data.json")
        scraped_data = [
            {
                "title": result.get("title", ""),
                "scraped_content": result.get("scraped_content", "")
            }
            for result in results
        ]
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(scraped_data, f, indent=2, ensure_ascii=False)
        print(f"\nScraped {len(results)} articles successfully")
        print(f"Results saved to '{output_file}'")
    else:
        print("\nNo results to save")