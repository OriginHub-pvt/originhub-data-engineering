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
from datetime import datetime, UTC

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)


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


def scrape_all_from_json_files(json_dir: str = "dags/filtered_data") -> List[Dict[str, Any]]:
    """
    Scrape all URLs from JSON files in the specified directory.
    
    Args:
        json_dir: Directory containing JSON files with feed data
        
    Returns:
        List of scraped results, each containing original metadata and scraped content
    """
    scraper = WebScraper()
    all_results = []
    
    # Get all JSON files from the directory
    if not os.path.exists(json_dir):
        logging.error(f"Directory {json_dir} does not exist")
        return []
    
    json_files = [f for f in os.listdir(json_dir) if f.endswith('.json')]
    
    if not json_files:
        logging.warning(f"No JSON files found in {json_dir}")
        return []
    
    logging.info(f"Found {len(json_files)} JSON files to process")
    
    for json_file in json_files:
        json_path = os.path.join(json_dir, json_file)
        logging.info(f"Processing file: {json_file}")
        
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                feed_items = json.load(f)
            
            if not isinstance(feed_items, list):
                logging.warning(f"{json_file} does not contain a list, skipping")
                continue
            
            logging.info(f"Found {len(feed_items)} URLs in {json_file}")
            
            for idx, item in enumerate(feed_items, 1):
                url = item.get('url')
                
                if not url:
                    logging.warning(f"No URL found in item {idx} of {json_file}, skipping")
                    continue
                
                try:
                    logging.info(f"Scraping [{idx}/{len(feed_items)}]: {url}")
                    
                    # Scrape the URL
                    scraped_data = scraper.scrape_full(url)
                    
                    # Combine original metadata with scraped content
                    result = {
                        **item,  # Original metadata (title, url, description, dates)
                        "scraped_content": scraped_data.get("text_content", ""),
                        "scraped_metadata": scraped_data.get("metadata", {}),
                        "scraped_at": scraped_data.get("fetched_at", ""),
                        "word_count": scraped_data.get("word_count", 0)
                    }
                    
                    all_results.append(result)
                    logging.info(f"✓ Successfully scraped: {item.get('title', url)}")
                    
                except Exception as e:
                    logging.error(f"✗ Failed to scrape {url}: {e}")
                    # Use description as fallback content when scraping fails
                    error_result = {
                        **item,
                        "scraped_content": item.get("description", ""),
                        "scrap_error": str(e),
                        "scraped_at": datetime.now(UTC).isoformat()
                    }
                    all_results.append(error_result)
        
        except Exception as e:
            logging.error(f"Error reading {json_file}: {e}")
            continue
    
    logging.info(f"Completed scraping. Total results: {len(all_results)}")
    
    # Save results to scraped_data directory
    if all_results:
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
            for result in all_results
        ]
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(scraped_data, f, indent=2, ensure_ascii=False)
        logging.info(f"Results saved to '{output_file}'")
    
    return all_results


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
        print(f"\n✓ Scraped {len(results)} articles successfully")
        print(f"✓ Results saved to '{output_file}'")
    else:
        print("\n✗ No results to save")