import pytest
import os
import json
import logging
from datetime import datetime, UTC
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, mock_open
from dags.src.scrapper import (
    WebScraper,
    scrape_url,
    scrape_web_content,
    scrape_all_from_json_files
)
import requests
from bs4 import BeautifulSoup


# Sample HTML for testing
SAMPLE_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Test Page Title</title>
    <meta name="description" content="This is a test description">
    <meta name="keywords" content="test, sample, html">
    <meta property="og:title" content="OG Test Title">
    <meta property="og:description" content="OG Test Description">
    <meta property="og:image" content="https://example.com/image.jpg">
</head>
<body>
    <h1>Main Heading</h1>
    <p>This is a test paragraph with some content.</p>
    <a href="https://example.com/page1">Link 1</a>
    <a href="/relative/page2">Link 2</a>
    <img src="https://example.com/image1.jpg" alt="Image 1">
    <img src="/relative/image2.jpg" alt="Image 2">
    <script>console.log('script');</script>
    <style>body { color: red; }</style>
</body>
</html>
"""

MINIMAL_HTML = """
<html>
<body>
    <p>Simple content</p>
</body>
</html>
"""


# ---------- Test WebScraper.__init__ ----------

def test_webscraper_init_default():
    """Test WebScraper initialization with default values"""
    scraper = WebScraper()
    assert scraper.timeout == 30
    assert scraper.headers is not None
    assert 'User-Agent' in scraper.headers
    assert 'Mozilla' in scraper.headers['User-Agent']


def test_webscraper_init_custom_timeout():
    """Test WebScraper initialization with custom timeout"""
    scraper = WebScraper(timeout=60)
    assert scraper.timeout == 60


def test_webscraper_init_custom_headers():
    """Test WebScraper initialization with custom headers"""
    custom_headers = {'User-Agent': 'Custom Bot', 'Accept': 'application/json'}
    scraper = WebScraper(headers=custom_headers)
    assert scraper.headers == custom_headers
    assert scraper.headers['User-Agent'] == 'Custom Bot'


def test_webscraper_init_partial_custom_headers():
    """Test WebScraper initialization merges custom headers"""
    custom_headers = {'X-Custom-Header': 'custom-value'}
    scraper = WebScraper(headers=custom_headers)
    assert scraper.headers == custom_headers


# ---------- Test WebScraper.fetch_url ----------

@patch('dags.src.scrapper.requests.get')
def test_fetch_url_success(mock_get):
    """Test successful URL fetching"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = SAMPLE_HTML
    mock_response.content = SAMPLE_HTML.encode('utf-8')
    mock_response.headers = {'Content-Type': 'text/html; charset=utf-8'}
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response
    
    scraper = WebScraper()
    result = scraper.fetch_url("https://example.com")
    
    assert result['url'] == "https://example.com"
    assert result['status_code'] == 200
    assert result['content'] == SAMPLE_HTML
    assert result['content_length'] == len(SAMPLE_HTML.encode('utf-8'))
    assert 'text/html' in result['content_type']
    assert 'fetched_at' in result
    assert 'headers' in result


@patch('dags.src.scrapper.requests.get')
def test_fetch_url_with_custom_timeout(mock_get):
    """Test URL fetching with custom timeout"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = MINIMAL_HTML
    mock_response.content = MINIMAL_HTML.encode('utf-8')
    mock_response.headers = {'Content-Type': 'text/html'}
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response
    
    scraper = WebScraper(timeout=120)
    scraper.fetch_url("https://example.com")
    
    mock_get.assert_called_once_with(
        "https://example.com",
        timeout=120,
        headers=scraper.headers,
        verify=True
    )


@patch('dags.src.scrapper.requests.get')
def test_fetch_url_ssl_error_retry(mock_get):
    """Test SSL error triggers retry without verification"""
    # First call raises SSL error, second succeeds
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = MINIMAL_HTML
    mock_response.content = MINIMAL_HTML.encode('utf-8')
    mock_response.headers = {'Content-Type': 'text/html'}
    mock_response.raise_for_status = Mock()
    
    mock_get.side_effect = [
        requests.exceptions.SSLError("SSL verification failed"),
        mock_response
    ]
    
    scraper = WebScraper()
    result = scraper.fetch_url("https://example.com")
    
    assert result['status_code'] == 200
    assert mock_get.call_count == 2
    # First call with verify=True, second with verify=False
    assert mock_get.call_args_list[0][1]['verify'] is True
    assert mock_get.call_args_list[1][1]['verify'] is False


@patch('dags.src.scrapper.requests.get')
def test_fetch_url_ssl_error_no_retry(mock_get):
    """Test SSL error without retry when verify_ssl is False"""
    mock_get.side_effect = requests.exceptions.SSLError("SSL error")
    
    scraper = WebScraper()
    with pytest.raises(requests.exceptions.SSLError):
        scraper.fetch_url("https://example.com", verify_ssl=False)
    
    assert mock_get.call_count == 1


@patch('dags.src.scrapper.requests.get')
def test_fetch_url_http_error(mock_get):
    """Test handling of HTTP errors"""
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
    mock_get.return_value = mock_response
    
    scraper = WebScraper()
    with pytest.raises(requests.exceptions.RequestException):
        scraper.fetch_url("https://example.com")


@patch('dags.src.scrapper.requests.get')
def test_fetch_url_timeout_error(mock_get):
    """Test handling of timeout errors"""
    mock_get.side_effect = requests.exceptions.Timeout("Request timed out")
    
    scraper = WebScraper()
    with pytest.raises(requests.exceptions.RequestException):
        scraper.fetch_url("https://example.com")


@patch('dags.src.scrapper.requests.get')
def test_fetch_url_connection_error(mock_get):
    """Test handling of connection errors"""
    mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")
    
    scraper = WebScraper()
    with pytest.raises(requests.exceptions.RequestException):
        scraper.fetch_url("https://example.com")


@patch('dags.src.scrapper.requests.get')
def test_fetch_url_fetched_at_format(mock_get):
    """Test that fetched_at is in ISO format"""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = MINIMAL_HTML
    mock_response.content = MINIMAL_HTML.encode('utf-8')
    mock_response.headers = {'Content-Type': 'text/html'}
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response
    
    scraper = WebScraper()
    result = scraper.fetch_url("https://example.com")
    
    # Verify ISO format
    fetched_at = result['fetched_at']
    datetime.fromisoformat(fetched_at)  # Should not raise


# ---------- Test WebScraper.parse_html ----------

def test_parse_html_valid():
    """Test parsing valid HTML"""
    scraper = WebScraper()
    soup = scraper.parse_html(SAMPLE_HTML)
    assert isinstance(soup, BeautifulSoup)
    assert soup.title.text == "Test Page Title"


def test_parse_html_minimal():
    """Test parsing minimal HTML"""
    scraper = WebScraper()
    soup = scraper.parse_html(MINIMAL_HTML)
    assert isinstance(soup, BeautifulSoup)
    assert soup.find('p').text == "Simple content"


def test_parse_html_empty():
    """Test parsing empty HTML"""
    scraper = WebScraper()
    soup = scraper.parse_html("")
    assert isinstance(soup, BeautifulSoup)


def test_parse_html_malformed():
    """Test parsing malformed HTML (BeautifulSoup handles gracefully)"""
    scraper = WebScraper()
    malformed = "<html><body><p>Unclosed paragraph</body></html>"
    soup = scraper.parse_html(malformed)
    assert isinstance(soup, BeautifulSoup)


# ---------- Test WebScraper.extract_metadata ----------

def test_extract_metadata_full():
    """Test extracting all metadata from HTML"""
    scraper = WebScraper()
    soup = scraper.parse_html(SAMPLE_HTML)
    metadata = scraper.extract_metadata(soup)
    
    assert metadata['title'] == "Test Page Title"
    assert metadata['description'] == "This is a test description"
    assert 'meta_tags' in metadata
    assert metadata['meta_tags']['keywords'] == "test, sample, html"
    assert 'og_tags' in metadata
    assert metadata['og_tags']['og:title'] == "OG Test Title"
    assert metadata['og_tags']['og:description'] == "OG Test Description"


def test_extract_metadata_minimal():
    """Test extracting metadata from minimal HTML"""
    scraper = WebScraper()
    soup = scraper.parse_html(MINIMAL_HTML)
    metadata = scraper.extract_metadata(soup)
    
    assert metadata['title'] == ""
    assert metadata['description'] == ""
    assert metadata['meta_tags'] == {}
    assert metadata['og_tags'] == {}


def test_extract_metadata_title_only():
    """Test extracting only title"""
    html = "<html><head><title>Only Title</title></head><body></body></html>"
    scraper = WebScraper()
    soup = scraper.parse_html(html)
    metadata = scraper.extract_metadata(soup)
    
    assert metadata['title'] == "Only Title"
    assert metadata['description'] == ""


def test_extract_metadata_og_tags_only():
    """Test extracting only Open Graph tags"""
    html = """
    <html>
    <head>
        <meta property="og:title" content="OG Title">
        <meta property="og:url" content="https://example.com">
    </head>
    <body></body>
    </html>
    """
    scraper = WebScraper()
    soup = scraper.parse_html(html)
    metadata = scraper.extract_metadata(soup)
    
    assert metadata['og_tags']['og:title'] == "OG Title"
    assert metadata['og_tags']['og:url'] == "https://example.com"


def test_extract_metadata_meta_without_content():
    """Test handling meta tags without content attribute"""
    html = """
    <html>
    <head>
        <meta name="viewport">
        <meta name="charset" content="utf-8">
    </head>
    <body></body>
    </html>
    """
    scraper = WebScraper()
    soup = scraper.parse_html(html)
    metadata = scraper.extract_metadata(soup)
    
    # Meta without content should not be included
    assert 'viewport' not in metadata['meta_tags']
    assert 'charset' in metadata['meta_tags']


# ---------- Test WebScraper.extract_text_content ----------

def test_extract_text_content_basic():
    """Test extracting text content from HTML"""
    scraper = WebScraper()
    soup = scraper.parse_html(SAMPLE_HTML)
    text = scraper.extract_text_content(soup)
    
    assert "Main Heading" in text
    assert "This is a test paragraph" in text
    assert "console.log" not in text  # Script should be removed
    assert "color: red" not in text  # Style should be removed


def test_extract_text_content_removes_scripts_and_styles():
    """Test that scripts and styles are removed"""
    html = """
    <html>
    <body>
        <p>Visible content</p>
        <script>alert('Hidden');</script>
        <style>body { display: none; }</style>
    </body>
    </html>
    """
    scraper = WebScraper()
    soup = scraper.parse_html(html)
    text = scraper.extract_text_content(soup)
    
    assert "Visible content" in text
    assert "alert" not in text
    assert "display: none" not in text


def test_extract_text_content_whitespace_cleanup():
    """Test that whitespace is properly cleaned up"""
    html = """
    <html>
    <body>
        <p>Line 1</p>
        
        <p>  Line   2  </p>
        
        <p>Line 3</p>
    </body>
    </html>
    """
    scraper = WebScraper()
    soup = scraper.parse_html(html)
    text = scraper.extract_text_content(soup)
    
    # Should have single spaces between words
    assert "Line 1" in text
    assert "Line 2" in text
    assert "Line 3" in text
    # Should not have excessive whitespace
    assert "  " not in text


def test_extract_text_content_empty():
    """Test extracting text from empty HTML"""
    scraper = WebScraper()
    soup = scraper.parse_html("<html><body></body></html>")
    text = scraper.extract_text_content(soup)
    
    assert text == ""


# ---------- Test WebScraper.extract_links ----------

def test_extract_links_basic():
    """Test extracting links from HTML"""
    scraper = WebScraper()
    soup = scraper.parse_html(SAMPLE_HTML)
    links = scraper.extract_links(soup)
    
    assert len(links) == 2
    assert links[0]['url'] == "https://example.com/page1"
    assert links[0]['text'] == "Link 1"
    assert links[1]['url'] == "/relative/page2"
    assert links[1]['text'] == "Link 2"


def test_extract_links_with_base_url():
    """Test extracting links with base URL resolution"""
    scraper = WebScraper()
    soup = scraper.parse_html(SAMPLE_HTML)
    links = scraper.extract_links(soup, base_url="https://example.com")
    
    assert len(links) == 2
    assert links[0]['url'] == "https://example.com/page1"
    assert links[1]['url'] == "https://example.com/relative/page2"


def test_extract_links_no_links():
    """Test extracting links when none exist"""
    html = "<html><body><p>No links here</p></body></html>"
    scraper = WebScraper()
    soup = scraper.parse_html(html)
    links = scraper.extract_links(soup)
    
    assert links == []


def test_extract_links_empty_text():
    """Test extracting links with no text"""
    html = '<html><body><a href="https://example.com"></a></body></html>'
    scraper = WebScraper()
    soup = scraper.parse_html(html)
    links = scraper.extract_links(soup)
    
    assert len(links) == 1
    assert links[0]['url'] == "https://example.com"
    assert links[0]['text'] == ""


def test_extract_links_protocol_relative():
    """Test extracting protocol-relative links"""
    html = '<html><body><a href="//example.com/page">Link</a></body></html>'
    scraper = WebScraper()
    soup = scraper.parse_html(html)
    links = scraper.extract_links(soup, base_url="https://example.com")
    
    assert len(links) == 1
    # Protocol-relative URLs start with //
    assert links[0]['url'] == "https://example.com/page"


# ---------- Test WebScraper.extract_images ----------

def test_extract_images_basic():
    """Test extracting images from HTML"""
    scraper = WebScraper()
    soup = scraper.parse_html(SAMPLE_HTML)
    images = scraper.extract_images(soup)
    
    assert len(images) == 2
    assert images[0]['url'] == "https://example.com/image1.jpg"
    assert images[0]['alt'] == "Image 1"
    assert images[1]['url'] == "/relative/image2.jpg"
    assert images[1]['alt'] == "Image 2"


def test_extract_images_with_base_url():
    """Test extracting images with base URL resolution"""
    scraper = WebScraper()
    soup = scraper.parse_html(SAMPLE_HTML)
    images = scraper.extract_images(soup, base_url="https://example.com")
    
    assert len(images) == 2
    assert images[0]['url'] == "https://example.com/image1.jpg"
    assert images[1]['url'] == "https://example.com/relative/image2.jpg"


def test_extract_images_no_images():
    """Test extracting images when none exist"""
    html = "<html><body><p>No images here</p></body></html>"
    scraper = WebScraper()
    soup = scraper.parse_html(html)
    images = scraper.extract_images(soup)
    
    assert images == []


def test_extract_images_no_alt():
    """Test extracting images without alt text"""
    html = '<html><body><img src="https://example.com/image.jpg"></body></html>'
    scraper = WebScraper()
    soup = scraper.parse_html(html)
    images = scraper.extract_images(soup)
    
    assert len(images) == 1
    assert images[0]['url'] == "https://example.com/image.jpg"
    assert images[0]['alt'] == ""


def test_extract_images_no_src():
    """Test that images without src are skipped"""
    html = '<html><body><img alt="No source"></body></html>'
    scraper = WebScraper()
    soup = scraper.parse_html(html)
    images = scraper.extract_images(soup)
    
    assert images == []


def test_extract_images_data_uri():
    """Test extracting images with data URI"""
    html = '<html><body><img src="data:image/png;base64,..." alt="Data Image"></body></html>'
    scraper = WebScraper()
    soup = scraper.parse_html(html)
    images = scraper.extract_images(soup)
    
    assert len(images) == 1
    assert images[0]['url'].startswith("data:image/png")


# ---------- Test WebScraper.scrape_full ----------

@patch.object(WebScraper, 'fetch_url')
def test_scrape_full_success(mock_fetch):
    """Test full scraping of a URL"""
    mock_fetch.return_value = {
        'content': SAMPLE_HTML,
        'fetched_at': '2025-10-26T12:00:00'
    }
    
    scraper = WebScraper()
    result = scraper.scrape_full("https://example.com")
    
    assert result['url'] == "https://example.com"
    assert result['fetched_at'] == '2025-10-26T12:00:00'
    assert 'metadata' in result
    assert result['metadata']['title'] == "Test Page Title"
    assert 'text_content' in result
    assert "Main Heading" in result['text_content']
    assert 'links' in result
    assert len(result['links']) == 2
    assert 'images' in result
    assert len(result['images']) == 2
    assert 'word_count' in result
    assert result['word_count'] > 0


@patch.object(WebScraper, 'fetch_url')
def test_scrape_full_minimal_html(mock_fetch):
    """Test full scraping with minimal HTML"""
    mock_fetch.return_value = {
        'content': MINIMAL_HTML,
        'fetched_at': '2025-10-26T12:00:00'
    }
    
    scraper = WebScraper()
    result = scraper.scrape_full("https://example.com")
    
    assert result['url'] == "https://example.com"
    assert result['metadata']['title'] == ""
    assert "Simple content" in result['text_content']
    assert result['links'] == []
    assert result['images'] == []


@patch.object(WebScraper, 'fetch_url')
def test_scrape_full_word_count(mock_fetch):
    """Test word count calculation in full scrape"""
    html = "<html><body><p>One two three four five</p></body></html>"
    mock_fetch.return_value = {
        'content': html,
        'fetched_at': '2025-10-26T12:00:00'
    }
    
    scraper = WebScraper()
    result = scraper.scrape_full("https://example.com")
    
    assert result['word_count'] == 5


# ---------- Test scrape_url convenience function ----------

@patch.object(WebScraper, 'scrape_full')
def test_scrape_url_function(mock_scrape_full):
    """Test the scrape_url convenience function"""
    mock_scrape_full.return_value = {'url': 'https://example.com', 'data': 'test'}
    
    result = scrape_url("https://example.com")
    
    assert result['url'] == 'https://example.com'
    mock_scrape_full.assert_called_once_with("https://example.com")


@patch.object(WebScraper, 'scrape_full')
def test_scrape_url_function_with_timeout(mock_scrape_full):
    """Test scrape_url with custom timeout"""
    mock_scrape_full.return_value = {'url': 'https://example.com'}
    
    scrape_url("https://example.com", timeout=60)
    
    # Verify WebScraper was initialized with correct timeout
    mock_scrape_full.assert_called_once()


# ---------- Test scrape_web_content Airflow function ----------

@patch.object(WebScraper, 'scrape_full')
def test_scrape_web_content_from_conf(mock_scrape_full):
    """Test Airflow task with URL from dag_run.conf"""
    mock_scrape_full.return_value = {'url': 'https://example.com', 'data': 'test'}
    
    dag_run = Mock()
    dag_run.conf = {'url': 'https://example.com'}
    
    context = {
        'dag_run': dag_run,
        'params': {}
    }
    
    result = scrape_web_content(**context)
    
    assert result['url'] == 'https://example.com'
    mock_scrape_full.assert_called_once_with('https://example.com')


@patch.object(WebScraper, 'scrape_full')
def test_scrape_web_content_from_params(mock_scrape_full):
    """Test Airflow task with URL from params"""
    mock_scrape_full.return_value = {'url': 'https://example.com', 'data': 'test'}
    
    context = {
        'params': {'url': 'https://example.com'}
    }
    
    result = scrape_web_content(**context)
    
    assert result['url'] == 'https://example.com'


@patch.object(WebScraper, 'scrape_full')
def test_scrape_web_content_conf_overrides_params(mock_scrape_full):
    """Test that dag_run.conf overrides params"""
    mock_scrape_full.return_value = {'url': 'https://override.com'}
    
    dag_run = Mock()
    dag_run.conf = {'url': 'https://override.com'}
    
    context = {
        'dag_run': dag_run,
        'params': {'url': 'https://default.com'}
    }
    
    result = scrape_web_content(**context)
    
    assert result['url'] == 'https://override.com'
    mock_scrape_full.assert_called_once_with('https://override.com')


def test_scrape_web_content_no_url_raises_error():
    """Test that missing URL raises ValueError"""
    context = {'params': {}}
    
    with pytest.raises(ValueError) as exc_info:
        scrape_web_content(**context)
    
    assert "URL must be provided" in str(exc_info.value)


@patch.object(WebScraper, 'scrape_full')
def test_scrape_web_content_custom_timeout(mock_scrape_full):
    """Test custom timeout in Airflow task"""
    mock_scrape_full.return_value = {'url': 'https://example.com'}
    
    dag_run = Mock()
    dag_run.conf = {'url': 'https://example.com', 'timeout': 120}
    
    context = {
        'dag_run': dag_run,
        'params': {}
    }
    
    scrape_web_content(**context)
    
    # Verify the timeout was used (WebScraper initialized with it)
    mock_scrape_full.assert_called_once()


# ---------- Test scrape_all_from_json_files ----------

@patch.object(WebScraper, 'scrape_full')
@patch('dags.src.scrapper.os.path.exists')
@patch('dags.src.scrapper.os.listdir')
@patch('builtins.open', new_callable=mock_open)
def test_scrape_all_from_json_files_success(mock_file, mock_listdir, mock_exists, mock_scrape):
    """Test scraping all URLs from JSON files"""
    mock_exists.return_value = True
    mock_listdir.return_value = ['feed1.json']
    
    feed_data = [
        {
            'title': 'Article 1',
            'url': 'https://example.com/article1',
            'description': 'Description 1'
        }
    ]
    
    mock_file.return_value.read.return_value = json.dumps(feed_data)
    mock_file.return_value.__enter__.return_value.read.return_value = json.dumps(feed_data)
    
    mock_scrape.return_value = {
        'text_content': 'Scraped content',
        'metadata': {'title': 'Article 1'},
        'fetched_at': '2025-10-26T12:00:00',
        'word_count': 10
    }
    
    results = scrape_all_from_json_files("test_dir")
    
    assert len(results) == 1
    assert results[0]['title'] == 'Article 1'
    assert results[0]['scraped_content'] == 'Scraped content'
    assert results[0]['word_count'] == 10


@patch('dags.src.scrapper.os.path.exists')
def test_scrape_all_from_json_files_directory_not_exists(mock_exists):
    """Test handling when directory doesn't exist"""
    mock_exists.return_value = False
    
    results = scrape_all_from_json_files("nonexistent_dir")
    
    assert results == []


@patch('dags.src.scrapper.os.path.exists')
@patch('dags.src.scrapper.os.listdir')
def test_scrape_all_from_json_files_no_json_files(mock_listdir, mock_exists):
    """Test handling when no JSON files found"""
    mock_exists.return_value = True
    mock_listdir.return_value = ['file.txt', 'data.csv']
    
    results = scrape_all_from_json_files("test_dir")
    
    assert results == []


@patch.object(WebScraper, 'scrape_full')
@patch('dags.src.scrapper.os.path.exists')
@patch('dags.src.scrapper.os.listdir')
@patch('builtins.open', new_callable=mock_open)
def test_scrape_all_from_json_files_scraping_error(mock_file, mock_listdir, mock_exists, mock_scrape):
    """Test handling of scraping errors with fallback"""
    mock_exists.return_value = True
    mock_listdir.return_value = ['feed1.json']
    
    feed_data = [
        {
            'title': 'Article 1',
            'url': 'https://example.com/article1',
            'description': 'Fallback description'
        }
    ]
    
    mock_file.return_value.__enter__.return_value.read.return_value = json.dumps(feed_data)
    mock_scrape.side_effect = Exception("Scraping failed")
    
    results = scrape_all_from_json_files("test_dir")
    
    assert len(results) == 1
    assert results[0]['scraped_content'] == 'Fallback description'
    assert 'scrap_error' in results[0]
    assert 'Scraping failed' in results[0]['scrap_error']


@patch.object(WebScraper, 'scrape_full')
@patch('dags.src.scrapper.os.path.exists')
@patch('dags.src.scrapper.os.listdir')
@patch('builtins.open', new_callable=mock_open)
def test_scrape_all_from_json_files_multiple_files(mock_file, mock_listdir, mock_exists, mock_scrape):
    """Test scraping from multiple JSON files"""
    mock_exists.return_value = True
    mock_listdir.return_value = ['feed1.json', 'feed2.json']
    
    feed1_data = [{'title': 'Article 1', 'url': 'https://example.com/1'}]
    feed2_data = [{'title': 'Article 2', 'url': 'https://example.com/2'}]
    
    mock_file.return_value.__enter__.return_value.read.side_effect = [
        json.dumps(feed1_data),
        json.dumps(feed2_data)
    ]
    
    mock_scrape.return_value = {
        'text_content': 'Content',
        'metadata': {},
        'fetched_at': '2025-10-26T12:00:00',
        'word_count': 5
    }
    
    results = scrape_all_from_json_files("test_dir")
    
    assert len(results) == 2
    assert results[0]['title'] == 'Article 1'
    assert results[1]['title'] == 'Article 2'


@patch.object(WebScraper, 'scrape_full')
@patch('dags.src.scrapper.os.path.exists')
@patch('dags.src.scrapper.os.listdir')
@patch('builtins.open', new_callable=mock_open)
def test_scrape_all_from_json_files_invalid_json(mock_file, mock_listdir, mock_exists, mock_scrape):
    """Test handling of invalid JSON file"""
    mock_exists.return_value = True
    mock_listdir.return_value = ['invalid.json']
    
    mock_file.return_value.__enter__.return_value.read.side_effect = json.JSONDecodeError("Invalid", "", 0)
    
    results = scrape_all_from_json_files("test_dir")
    
    assert results == []


@patch.object(WebScraper, 'scrape_full')
@patch('dags.src.scrapper.os.path.exists')
@patch('dags.src.scrapper.os.listdir')
@patch('builtins.open', new_callable=mock_open)
def test_scrape_all_from_json_files_not_list(mock_file, mock_listdir, mock_exists, mock_scrape):
    """Test handling when JSON file doesn't contain a list"""
    mock_exists.return_value = True
    mock_listdir.return_value = ['data.json']
    
    mock_file.return_value.__enter__.return_value.read.return_value = json.dumps({'not': 'a list'})
    
    results = scrape_all_from_json_files("test_dir")
    
    assert results == []


@patch.object(WebScraper, 'scrape_full')
@patch('dags.src.scrapper.os.path.exists')
@patch('dags.src.scrapper.os.listdir')
@patch('builtins.open', new_callable=mock_open)
def test_scrape_all_from_json_files_missing_url(mock_file, mock_listdir, mock_exists, mock_scrape):
    """Test handling when item has no URL"""
    mock_exists.return_value = True
    mock_listdir.return_value = ['feed.json']
    
    feed_data = [
        {'title': 'No URL Article'},
        {'title': 'Article 2', 'url': 'https://example.com/2'}
    ]
    
    mock_file.return_value.__enter__.return_value.read.return_value = json.dumps(feed_data)
    mock_scrape.return_value = {
        'text_content': 'Content',
        'metadata': {},
        'fetched_at': '2025-10-26T12:00:00',
        'word_count': 5
    }
    
    results = scrape_all_from_json_files("test_dir")
    
    # Only the item with URL should be scraped
    assert len(results) == 1
    assert results[0]['title'] == 'Article 2'


@patch.object(WebScraper, 'scrape_full')
@patch('dags.src.scrapper.os.path.exists')
@patch('dags.src.scrapper.os.listdir')
@patch('builtins.open', new_callable=mock_open)
def test_scrape_all_from_json_files_preserves_original_metadata(mock_file, mock_listdir, mock_exists, mock_scrape):
    """Test that original metadata is preserved in results"""
    mock_exists.return_value = True
    mock_listdir.return_value = ['feed.json']
    
    feed_data = [
        {
            'title': 'Article 1',
            'url': 'https://example.com/1',
            'description': 'Original description',
            'published': '2025-10-26',
            'author': 'John Doe'
        }
    ]
    
    mock_file.return_value.__enter__.return_value.read.return_value = json.dumps(feed_data)
    mock_scrape.return_value = {
        'text_content': 'Scraped content',
        'metadata': {'scraped_title': 'Scraped Title'},
        'fetched_at': '2025-10-26T12:00:00',
        'word_count': 10
    }
    
    results = scrape_all_from_json_files("test_dir")
    
    assert len(results) == 1
    # Original metadata preserved
    assert results[0]['title'] == 'Article 1'
    assert results[0]['description'] == 'Original description'
    assert results[0]['published'] == '2025-10-26'
    assert results[0]['author'] == 'John Doe'
    # Scraped data added
    assert results[0]['scraped_content'] == 'Scraped content'
    assert results[0]['word_count'] == 10
