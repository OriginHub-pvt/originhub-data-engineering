import pytest
import os
import logging
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from dags.src.fetch_store_rss import (
    _normalize_urls,
    _slug_from_url,
    _build_filename,
    _resolve_output_dir,
    _extract_config,
    fetch_rss_feeds,
    store_rss_feeds,
    DEFAULT_TIMEOUT
)
import requests


# ---------- Test _normalize_urls ----------

def test_normalize_urls_with_none():
    """Test that None returns an empty list"""
    assert _normalize_urls(None) == []


def test_normalize_urls_with_single_string():
    """Test normalizing a single URL string"""
    result = _normalize_urls("https://example.com/feed")
    assert result == ["https://example.com/feed"]


def test_normalize_urls_with_comma_separated_string():
    """Test normalizing comma-separated URLs in a string"""
    result = _normalize_urls("https://example.com/feed1, https://example.com/feed2")
    assert result == ["https://example.com/feed1", "https://example.com/feed2"]


def test_normalize_urls_with_newline_separated_string():
    """Test normalizing newline-separated URLs in a string"""
    result = _normalize_urls("https://example.com/feed1\nhttps://example.com/feed2")
    assert result == ["https://example.com/feed1", "https://example.com/feed2"]


def test_normalize_urls_with_mixed_separators():
    """Test normalizing URLs with both comma and newline separators"""
    result = _normalize_urls("https://example.com/feed1,https://example.com/feed2\nhttps://example.com/feed3")
    assert result == ["https://example.com/feed1", "https://example.com/feed2", "https://example.com/feed3"]


def test_normalize_urls_with_list():
    """Test normalizing a list of URLs"""
    result = _normalize_urls(["https://example.com/feed1", "https://example.com/feed2"])
    assert result == ["https://example.com/feed1", "https://example.com/feed2"]


def test_normalize_urls_with_list_containing_numbers():
    """Test normalizing a list with non-string items"""
    result = _normalize_urls([123, "https://example.com/feed"])
    assert result == ["123", "https://example.com/feed"]


def test_normalize_urls_with_whitespace():
    """Test that whitespace is properly trimmed"""
    result = _normalize_urls("  https://example.com/feed1  ,  https://example.com/feed2  ")
    assert result == ["https://example.com/feed1", "https://example.com/feed2"]


def test_normalize_urls_with_empty_string():
    """Test that empty strings return empty list"""
    assert _normalize_urls("") == []
    assert _normalize_urls("   ") == []


def test_normalize_urls_with_empty_list():
    """Test that empty list returns empty list"""
    assert _normalize_urls([]) == []


def test_normalize_urls_with_list_containing_empty_strings():
    """Test filtering out empty strings from list"""
    result = _normalize_urls(["https://example.com/feed", "", "  ", "https://example.com/feed2"])
    assert result == ["https://example.com/feed", "https://example.com/feed2"]


def test_normalize_urls_with_non_iterable_object():
    """Test normalizing non-iterable object converts to string"""
    result = _normalize_urls(12345)
    assert result == ["12345"]


# ---------- Test _slug_from_url ----------

def test_slug_from_url_basic():
    """Test creating slug from basic URL"""
    result = _slug_from_url("https://example.com/rss/feed.xml")
    assert result == "example.com_rss_feed.xml"


def test_slug_from_url_without_path():
    """Test creating slug from URL without path"""
    result = _slug_from_url("https://example.com")
    assert result == "example.com"


def test_slug_from_url_with_subdomain():
    """Test creating slug from URL with subdomain"""
    result = _slug_from_url("https://blog.example.com/feed")
    assert result == "blog.example.com_feed"


def test_slug_from_url_with_port():
    """Test creating slug from URL with port"""
    result = _slug_from_url("https://example.com:8080/feed")
    assert result == "example.com_8080_feed"


def test_slug_from_url_with_special_characters():
    """Test that special characters are replaced with underscores"""
    result = _slug_from_url("https://example.com/feed?query=test&page=1")
    assert "_" in result
    assert "?" not in result
    assert "&" not in result
    assert "=" not in result


def test_slug_from_url_with_multiple_path_segments():
    """Test creating slug from URL with multiple path segments"""
    result = _slug_from_url("https://example.com/blog/2025/10/feed.xml")
    assert result == "example.com_blog_2025_10_feed.xml"


def test_slug_from_url_with_trailing_slash():
    """Test that trailing slash doesn't create empty segment"""
    result = _slug_from_url("https://example.com/feed/")
    assert result == "example.com_feed"
    assert not result.endswith("_")


def test_slug_from_url_with_hyphens():
    """Test that hyphens are preserved in slugs"""
    result = _slug_from_url("https://my-blog.example-site.com/my-feed")
    assert "my-blog.example-site.com_my-feed" == result


def test_slug_from_url_with_dots_in_path():
    """Test that dots in path are preserved"""
    result = _slug_from_url("https://example.com/feed.rss.xml")
    assert "example.com_feed.rss.xml" == result


def test_slug_from_url_fallback_to_netloc():
    """Test fallback when URL has no path"""
    result = _slug_from_url("https://example.com/")
    assert result == "example.com"


# ---------- Test _build_filename ----------

def test_build_filename_basic():
    """Test building filename with URL and timestamp"""
    fetched_at = datetime(2025, 10, 26, 14, 30, 45)
    result = _build_filename("https://example.com/feed", fetched_at)
    assert result.startswith("20251026143045_")
    assert result.endswith(".xml")
    assert "example.com_feed" in result


def test_build_filename_different_timestamps():
    """Test that different timestamps produce different filenames"""
    url = "https://example.com/feed"
    timestamp1 = datetime(2025, 10, 26, 14, 30, 45)
    timestamp2 = datetime(2025, 10, 26, 15, 30, 45)
    
    result1 = _build_filename(url, timestamp1)
    result2 = _build_filename(url, timestamp2)
    
    assert result1 != result2
    assert result1.startswith("20251026143045_")
    assert result2.startswith("20251026153045_")


def test_build_filename_different_urls():
    """Test that different URLs produce different filenames"""
    timestamp = datetime(2025, 10, 26, 14, 30, 45)
    result1 = _build_filename("https://example1.com/feed", timestamp)
    result2 = _build_filename("https://example2.com/feed", timestamp)
    
    assert result1 != result2
    assert "example1.com" in result1
    assert "example2.com" in result2


def test_build_filename_format():
    """Test filename format is correct"""
    fetched_at = datetime(2025, 1, 1, 0, 0, 0)
    result = _build_filename("https://example.com/feed", fetched_at)
    assert result == "20250101000000_example.com_feed.xml"


# ---------- Test _resolve_output_dir ----------

def test_resolve_output_dir_with_none(tmp_path, monkeypatch):
    """Test that None defaults to 'rss_data' directory"""
    # Mock __file__ to control the base directory
    test_file = tmp_path / "dags" / "src" / "fetch_store_rss.py"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.touch()
    
    with patch('dags.src.fetch_store_rss.__file__', str(test_file)):
        result = _resolve_output_dir(None)
        assert result.name == "rss_data"
        assert result.exists()
        assert result.is_dir()


def test_resolve_output_dir_with_relative_path(tmp_path, monkeypatch):
    """Test resolving relative output directory"""
    test_file = tmp_path / "dags" / "src" / "fetch_store_rss.py"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.touch()
    
    with patch('dags.src.fetch_store_rss.__file__', str(test_file)):
        result = _resolve_output_dir("custom_data")
        assert result.name == "custom_data"
        assert result.parent == tmp_path / "dags"
        assert result.exists()


def test_resolve_output_dir_with_absolute_path(tmp_path):
    """Test resolving absolute output directory"""
    abs_path = tmp_path / "absolute_rss_data"
    result = _resolve_output_dir(str(abs_path))
    assert result == abs_path
    assert result.exists()
    assert result.is_dir()


def test_resolve_output_dir_creates_nested_directories(tmp_path):
    """Test that nested directories are created"""
    nested_path = tmp_path / "level1" / "level2" / "level3"
    result = _resolve_output_dir(str(nested_path))
    assert result == nested_path
    assert result.exists()
    assert result.is_dir()


def test_resolve_output_dir_idempotent(tmp_path):
    """Test that calling twice doesn't raise error"""
    path = tmp_path / "test_data"
    result1 = _resolve_output_dir(str(path))
    result2 = _resolve_output_dir(str(path))
    assert result1 == result2
    assert result1.exists()


# ---------- Test _extract_config ----------

def test_extract_config_from_params():
    """Test extracting configuration from params"""
    context = {
        "params": {
            "rss_url": "https://example.com/feed",
            "output_dir": "/tmp/rss",
            "request_timeout": 45
        }
    }
    urls, output_dir, timeout = _extract_config(context)
    assert urls == ["https://example.com/feed"]
    assert output_dir == "/tmp/rss"
    assert timeout == 45


def test_extract_config_from_dag_run_conf():
    """Test extracting configuration from dag_run.conf"""
    dag_run = Mock()
    dag_run.conf = {
        "rss_url": "https://example.com/feed",
        "output_dir": "/tmp/rss",
        "request_timeout": 60
    }
    context = {"dag_run": dag_run, "params": {}}
    
    urls, output_dir, timeout = _extract_config(context)
    assert urls == ["https://example.com/feed"]
    assert output_dir == "/tmp/rss"
    assert timeout == 60


def test_extract_config_dag_run_overrides_params():
    """Test that dag_run.conf overrides params"""
    dag_run = Mock()
    dag_run.conf = {
        "rss_url": "https://override.com/feed",
        "request_timeout": 90
    }
    context = {
        "dag_run": dag_run,
        "params": {
            "rss_url": "https://default.com/feed",
            "output_dir": "/tmp/rss",
            "request_timeout": 30
        }
    }
    
    urls, output_dir, timeout = _extract_config(context)
    assert urls == ["https://override.com/feed"]
    assert output_dir == "/tmp/rss"  # Not overridden
    assert timeout == 90


def test_extract_config_default_timeout():
    """Test that default timeout is used when not specified"""
    context = {
        "params": {
            "rss_url": "https://example.com/feed"
        }
    }
    urls, output_dir, timeout = _extract_config(context)
    assert timeout == DEFAULT_TIMEOUT


def test_extract_config_no_dag_run():
    """Test extracting config when dag_run is None"""
    context = {
        "params": {
            "rss_url": "https://example.com/feed"
        },
        "dag_run": None
    }
    urls, output_dir, timeout = _extract_config(context)
    assert urls == ["https://example.com/feed"]
    assert output_dir is None
    assert timeout == DEFAULT_TIMEOUT


def test_extract_config_empty_context():
    """Test extracting config from empty context"""
    context = {}
    urls, output_dir, timeout = _extract_config(context)
    assert urls == []
    assert output_dir is None
    assert timeout == DEFAULT_TIMEOUT


def test_extract_config_multiple_urls():
    """Test extracting multiple URLs from config"""
    context = {
        "params": {
            "rss_url": "https://example1.com/feed,https://example2.com/feed"
        }
    }
    urls, output_dir, timeout = _extract_config(context)
    assert len(urls) == 2
    assert "https://example1.com/feed" in urls
    assert "https://example2.com/feed" in urls


def test_extract_config_timeout_as_string():
    """Test that timeout string is converted to integer"""
    context = {
        "params": {
            "rss_url": "https://example.com/feed",
            "request_timeout": "120"
        }
    }
    urls, output_dir, timeout = _extract_config(context)
    assert timeout == 120
    assert isinstance(timeout, int)


# ---------- Test fetch_rss_feeds ----------

@patch('dags.src.fetch_store_rss.requests.get')
@patch('dags.src.fetch_store_rss.datetime')
def test_fetch_rss_feeds_success(mock_datetime, mock_get):
    """Test successful RSS feed fetching"""
    # Setup mocks
    fixed_time = datetime(2025, 10, 26, 12, 0, 0)
    mock_datetime.utcnow.return_value = fixed_time
    
    mock_response = Mock()
    mock_response.text = '<?xml version="1.0"?><rss><channel><title>Test</title></channel></rss>'
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response
    
    context = {
        "params": {
            "rss_url": "https://example.com/feed",
            "output_dir": "/tmp/rss"
        }
    }
    
    result = fetch_rss_feeds(**context)
    
    # Assertions
    assert "feeds" in result
    assert "output_dir" in result
    assert len(result["feeds"]) == 1
    assert result["feeds"][0]["rss_url"] == "https://example.com/feed"
    assert result["feeds"][0]["content"] == mock_response.text
    assert result["feeds"][0]["fetched_at"] == "2025-10-26T12:00:00"
    assert result["output_dir"] == "/tmp/rss"
    
    mock_get.assert_called_once_with("https://example.com/feed", timeout=DEFAULT_TIMEOUT)


@patch('dags.src.fetch_store_rss.requests.get')
@patch('dags.src.fetch_store_rss.datetime')
def test_fetch_rss_feeds_multiple_urls(mock_datetime, mock_get):
    """Test fetching multiple RSS feeds"""
    fixed_time = datetime(2025, 10, 26, 12, 0, 0)
    mock_datetime.utcnow.return_value = fixed_time
    
    mock_response = Mock()
    mock_response.text = '<rss><channel><title>Test</title></channel></rss>'
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response
    
    context = {
        "params": {
            "rss_url": ["https://example1.com/feed", "https://example2.com/feed"]
        }
    }
    
    result = fetch_rss_feeds(**context)
    
    assert len(result["feeds"]) == 2
    assert result["feeds"][0]["rss_url"] == "https://example1.com/feed"
    assert result["feeds"][1]["rss_url"] == "https://example2.com/feed"
    assert mock_get.call_count == 2


@patch('dags.src.fetch_store_rss.requests.get')
def test_fetch_rss_feeds_no_url_raises_error(mock_get):
    """Test that missing URL raises ValueError"""
    context = {"params": {}}
    
    with pytest.raises(ValueError) as exc_info:
        fetch_rss_feeds(**context)
    
    assert "at least one RSS URL" in str(exc_info.value)
    mock_get.assert_not_called()


@patch('dags.src.fetch_store_rss.requests.get')
def test_fetch_rss_feeds_request_exception(mock_get):
    """Test handling of request exceptions"""
    mock_get.side_effect = requests.exceptions.RequestException("Connection error")
    
    context = {
        "params": {
            "rss_url": "https://example.com/feed"
        }
    }
    
    with pytest.raises(Exception):
        fetch_rss_feeds(**context)


@patch('dags.src.fetch_store_rss.requests.get')
def test_fetch_rss_feeds_http_error(mock_get):
    """Test handling of HTTP errors"""
    mock_response = Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
    mock_get.return_value = mock_response
    
    context = {
        "params": {
            "rss_url": "https://example.com/feed"
        }
    }
    
    with pytest.raises(Exception):
        fetch_rss_feeds(**context)


@patch('dags.src.fetch_store_rss.requests.get')
@patch('dags.src.fetch_store_rss.datetime')
def test_fetch_rss_feeds_custom_timeout(mock_datetime, mock_get):
    """Test using custom timeout value"""
    fixed_time = datetime(2025, 10, 26, 12, 0, 0)
    mock_datetime.utcnow.return_value = fixed_time
    
    mock_response = Mock()
    mock_response.text = '<rss></rss>'
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response
    
    context = {
        "params": {
            "rss_url": "https://example.com/feed",
            "request_timeout": 120
        }
    }
    
    fetch_rss_feeds(**context)
    
    mock_get.assert_called_once_with("https://example.com/feed", timeout=120)


@patch('dags.src.fetch_store_rss.requests.get')
@patch('dags.src.fetch_store_rss.datetime')
def test_fetch_rss_feeds_strips_microseconds(mock_datetime, mock_get):
    """Test that microseconds are stripped from timestamp"""
    fixed_time = datetime(2025, 10, 26, 12, 0, 0, 123456)
    mock_datetime.utcnow.return_value = fixed_time
    
    mock_response = Mock()
    mock_response.text = '<rss></rss>'
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response
    
    context = {
        "params": {
            "rss_url": "https://example.com/feed"
        }
    }
    
    result = fetch_rss_feeds(**context)
    
    # Verify microseconds are removed
    assert result["feeds"][0]["fetched_at"] == "2025-10-26T12:00:00"


@patch('dags.src.fetch_store_rss.requests.get')
def test_fetch_rss_feeds_timeout_exception(mock_get):
    """Test handling of timeout exceptions"""
    mock_get.side_effect = requests.exceptions.Timeout("Request timed out")
    
    context = {
        "params": {
            "rss_url": "https://example.com/feed"
        }
    }
    
    with pytest.raises(Exception):
        fetch_rss_feeds(**context)


# ---------- Test store_rss_feeds ----------

def test_store_rss_feeds_success(tmp_path):
    """Test successful storing of RSS feeds"""
    payload = {
        "feeds": [
            {
                "rss_url": "https://example.com/feed",
                "content": "<?xml version='1.0'?><rss><channel><title>Test</title></channel></rss>",
                "fetched_at": "2025-10-26T12:00:00"
            }
        ],
        "output_dir": str(tmp_path)
    }
    
    result = store_rss_feeds(payload)
    
    assert len(result) == 1
    assert Path(result[0]).exists()
    assert Path(result[0]).parent == tmp_path
    
    # Verify content
    content = Path(result[0]).read_text(encoding="utf-8")
    assert "Test" in content


def test_store_rss_feeds_multiple_feeds(tmp_path):
    """Test storing multiple RSS feeds"""
    payload = {
        "feeds": [
            {
                "rss_url": "https://example1.com/feed",
                "content": "<rss><channel><title>Feed 1</title></channel></rss>",
                "fetched_at": "2025-10-26T12:00:00"
            },
            {
                "rss_url": "https://example2.com/feed",
                "content": "<rss><channel><title>Feed 2</title></channel></rss>",
                "fetched_at": "2025-10-26T12:01:00"
            }
        ],
        "output_dir": str(tmp_path)
    }
    
    result = store_rss_feeds(payload)
    
    assert len(result) == 2
    assert all(Path(p).exists() for p in result)
    assert Path(result[0]).read_text().count("Feed 1") == 1
    assert Path(result[1]).read_text().count("Feed 2") == 1


def test_store_rss_feeds_no_feeds_raises_error():
    """Test that empty feeds list raises ValueError"""
    payload = {"feeds": []}
    
    with pytest.raises(ValueError) as exc_info:
        store_rss_feeds(payload)
    
    assert "No feed payloads supplied" in str(exc_info.value)


def test_store_rss_feeds_missing_feeds_key_raises_error():
    """Test that missing feeds key raises ValueError"""
    payload = {"output_dir": "/tmp"}
    
    with pytest.raises(ValueError) as exc_info:
        store_rss_feeds(payload)
    
    assert "No feed payloads supplied" in str(exc_info.value)


def test_store_rss_feeds_creates_output_directory(tmp_path):
    """Test that output directory is created if it doesn't exist"""
    output_dir = tmp_path / "new_directory"
    assert not output_dir.exists()
    
    payload = {
        "feeds": [
            {
                "rss_url": "https://example.com/feed",
                "content": "<rss></rss>",
                "fetched_at": "2025-10-26T12:00:00"
            }
        ],
        "output_dir": str(output_dir)
    }
    
    result = store_rss_feeds(payload)
    
    assert output_dir.exists()
    assert output_dir.is_dir()
    assert len(result) == 1


def test_store_rss_feeds_without_fetched_at(tmp_path):
    """Test storing feed without fetched_at uses current time"""
    payload = {
        "feeds": [
            {
                "rss_url": "https://example.com/feed",
                "content": "<rss></rss>"
            }
        ],
        "output_dir": str(tmp_path)
    }
    
    result = store_rss_feeds(payload)
    
    assert len(result) == 1
    assert Path(result[0]).exists()


def test_store_rss_feeds_filename_format(tmp_path):
    """Test that stored files have correct filename format"""
    payload = {
        "feeds": [
            {
                "rss_url": "https://example.com/blog/feed",
                "content": "<rss></rss>",
                "fetched_at": "2025-10-26T14:30:45"
            }
        ],
        "output_dir": str(tmp_path)
    }
    
    result = store_rss_feeds(payload)
    
    filename = Path(result[0]).name
    assert filename.startswith("20251026143045_")
    assert filename.endswith(".xml")
    assert "example.com" in filename


def test_store_rss_feeds_with_relative_output_dir(tmp_path, monkeypatch):
    """Test storing with relative output directory"""
    test_file = tmp_path / "dags" / "src" / "fetch_store_rss.py"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.touch()
    
    with patch('dags.src.fetch_store_rss.__file__', str(test_file)):
        payload = {
            "feeds": [
                {
                    "rss_url": "https://example.com/feed",
                    "content": "<rss></rss>",
                    "fetched_at": "2025-10-26T12:00:00"
                }
            ],
            "output_dir": "relative_rss_data"
        }
        
        result = store_rss_feeds(payload)
        
        assert len(result) == 1
        assert Path(result[0]).exists()


def test_store_rss_feeds_unicode_content(tmp_path):
    """Test storing feeds with unicode content"""
    payload = {
        "feeds": [
            {
                "rss_url": "https://example.com/feed",
                "content": "<rss><channel><title>Test 中文 العربية 🎉</title></channel></rss>",
                "fetched_at": "2025-10-26T12:00:00"
            }
        ],
        "output_dir": str(tmp_path)
    }
    
    result = store_rss_feeds(payload)
    
    content = Path(result[0]).read_text(encoding="utf-8")
    assert "中文" in content
    assert "العربية" in content
    assert "🎉" in content


def test_store_rss_feeds_large_content(tmp_path):
    """Test storing feeds with large content"""
    large_content = "<rss>" + ("x" * 1000000) + "</rss>"  # 1MB of content
    
    payload = {
        "feeds": [
            {
                "rss_url": "https://example.com/feed",
                "content": large_content,
                "fetched_at": "2025-10-26T12:00:00"
            }
        ],
        "output_dir": str(tmp_path)
    }
    
    result = store_rss_feeds(payload)
    
    assert len(result) == 1
    assert Path(result[0]).stat().st_size > 1000000


def test_store_rss_feeds_returns_absolute_paths(tmp_path):
    """Test that returned paths are absolute"""
    payload = {
        "feeds": [
            {
                "rss_url": "https://example.com/feed",
                "content": "<rss></rss>",
                "fetched_at": "2025-10-26T12:00:00"
            }
        ],
        "output_dir": str(tmp_path)
    }
    
    result = store_rss_feeds(payload)
    
    for path in result:
        assert Path(path).is_absolute() or path.startswith(str(tmp_path))


def test_store_rss_feeds_io_error_handling(tmp_path, caplog):
    """Test handling of IO errors during file write"""
    # Create a read-only directory
    readonly_dir = tmp_path / "readonly"
    readonly_dir.mkdir()
    readonly_dir.chmod(0o444)
    
    payload = {
        "feeds": [
            {
                "rss_url": "https://example.com/feed",
                "content": "<rss></rss>",
                "fetched_at": "2025-10-26T12:00:00"
            }
        ],
        "output_dir": str(readonly_dir)
    }
    
    with caplog.at_level(logging.ERROR):
        # This should not raise, but log the error
        result = store_rss_feeds(payload)
        
        # Result should be empty or not include the failed file
        # depending on implementation
        assert isinstance(result, list)
    
    # Cleanup
    readonly_dir.chmod(0o755)


def test_store_rss_feeds_default_output_dir(tmp_path, monkeypatch):
    """Test storing with default output directory (None)"""
    test_file = tmp_path / "dags" / "src" / "fetch_store_rss.py"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.touch()
    
    with patch('dags.src.fetch_store_rss.__file__', str(test_file)):
        payload = {
            "feeds": [
                {
                    "rss_url": "https://example.com/feed",
                    "content": "<rss></rss>",
                    "fetched_at": "2025-10-26T12:00:00"
                }
            ],
            "output_dir": None
        }
        
        result = store_rss_feeds(payload)
        
        assert len(result) == 1
        assert Path(result[0]).exists()
        assert "rss_data" in result[0]


# ---------- Integration Tests ----------

@patch('dags.src.fetch_store_rss.requests.get')
@patch('dags.src.fetch_store_rss.datetime')
def test_fetch_and_store_integration(mock_datetime, mock_get, tmp_path):
    """Test integration of fetch and store operations"""
    # Setup mocks
    fixed_time = datetime(2025, 10, 26, 12, 0, 0)
    mock_datetime.utcnow.return_value = fixed_time
    
    mock_response = Mock()
    mock_response.text = '<?xml version="1.0"?><rss><channel><title>Integration Test</title></channel></rss>'
    mock_response.raise_for_status = Mock()
    mock_get.return_value = mock_response
    
    # Fetch
    context = {
        "params": {
            "rss_url": "https://example.com/feed",
            "output_dir": str(tmp_path)
        }
    }
    fetch_result = fetch_rss_feeds(**context)
    
    # Store
    store_result = store_rss_feeds(fetch_result)
    
    # Verify
    assert len(store_result) == 1
    assert Path(store_result[0]).exists()
    content = Path(store_result[0]).read_text()
    assert "Integration Test" in content


@patch('dags.src.fetch_store_rss.requests.get')
@patch('dags.src.fetch_store_rss.datetime')
def test_fetch_and_store_multiple_feeds_integration(mock_datetime, mock_get, tmp_path):
    """Test integration with multiple feeds"""
    fixed_time = datetime(2025, 10, 26, 12, 0, 0)
    mock_datetime.utcnow.return_value = fixed_time
    
    def mock_get_response(url, timeout):
        mock_response = Mock()
        if "feed1" in url:
            mock_response.text = '<rss><channel><title>Feed 1</title></channel></rss>'
        else:
            mock_response.text = '<rss><channel><title>Feed 2</title></channel></rss>'
        mock_response.raise_for_status = Mock()
        return mock_response
    
    mock_get.side_effect = mock_get_response
    
    context = {
        "params": {
            "rss_url": ["https://example.com/feed1", "https://example.com/feed2"],
            "output_dir": str(tmp_path)
        }
    }
    
    fetch_result = fetch_rss_feeds(**context)
    store_result = store_rss_feeds(fetch_result)
    
    assert len(store_result) == 2
    assert all(Path(p).exists() for p in store_result)
