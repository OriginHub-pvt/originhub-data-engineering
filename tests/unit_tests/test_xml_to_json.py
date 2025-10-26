import pytest
import os
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from dags.src.xml_to_json import (
    _to_iso_from_struct, 
    _strip_html, 
    _first_url_from_links,
    _best_url,
    _best_created,
    _best_updated,
    _coalesce_description,
    normalize_entry, 
    parse_xml_file, 
    dedupe_by_url, 
    normalize_all_feeds,
    normalize_latest_feeds,
    setup_logging
)
import logging
import feedparser


# ---------- Test setup_logging ----------

def test_setup_logging():
    """Test that setup_logging configures logging correctly"""
    setup_logging()
    logger = logging.getLogger()
    assert logger.level == logging.DEBUG


# ---------- Test _to_iso_from_struct ----------

def test_to_iso_from_struct_with_valid_time():
    """Test converting valid time struct to ISO format"""
    time_struct = datetime(2025, 10, 26, 12, 0, 0).timetuple()
    result = _to_iso_from_struct(time_struct)
    assert result == "2025-10-26T12:00:00Z"


def test_to_iso_from_struct_with_none():
    """Test that None returns None"""
    assert _to_iso_from_struct(None) is None


def test_to_iso_from_struct_with_zero():
    """Test that zero/false values return None"""
    assert _to_iso_from_struct(0) is None
    assert _to_iso_from_struct(False) is None


def test_to_iso_from_struct_with_different_times():
    """Test various time values"""
    # Midnight
    result = _to_iso_from_struct(datetime(2025, 1, 1, 0, 0, 0).timetuple())
    assert result == "2025-01-01T00:00:00Z"
    
    # End of day
    result = _to_iso_from_struct(datetime(2025, 12, 31, 23, 59, 59).timetuple())
    assert result == "2025-12-31T23:59:59Z"


def test_to_iso_from_struct_with_malformed_structure():
    """Test that malformed time structure returns None"""
    assert _to_iso_from_struct(object()) is None
    assert _to_iso_from_struct("invalid") is None
    assert _to_iso_from_struct({}) is None


def test_to_iso_from_struct_timezone_handling():
    """Test that timezone is always UTC"""
    time_struct = datetime(2025, 6, 15, 14, 30, 45).timetuple()
    result = _to_iso_from_struct(time_struct)
    assert result.endswith("Z")
    assert "+" not in result


# ---------- Test _strip_html ----------

def test_strip_html_with_simple_tags():
    """Test stripping simple HTML tags"""
    assert _strip_html('<p>Test</p>') == 'Test'
    assert _strip_html('<div>Content</div>') == 'Content'
    assert _strip_html('<span>Text</span>') == 'Text'


def test_strip_html_with_none():
    """Test that None returns empty string"""
    assert _strip_html(None) == ''


def test_strip_html_with_empty_string():
    """Test that empty string returns empty string"""
    assert _strip_html('') == ''
    assert _strip_html('   ') == ''


def test_strip_html_with_complex_html():
    """Test stripping complex HTML with multiple tags"""
    html = '<p><b>Bold</b> and <a href="#">link</a></p>'
    result = _strip_html(html)
    assert result == 'Bold and link'


def test_strip_html_with_nested_tags():
    """Test stripping deeply nested HTML"""
    html = '<div><p><span><strong>Nested</strong></span></p></div>'
    result = _strip_html(html)
    assert result == 'Nested'


def test_strip_html_with_attributes():
    """Test stripping HTML with various attributes"""
    html = '<a href="http://example.com" class="link" id="test">Link Text</a>'
    result = _strip_html(html)
    assert result == 'Link Text'
    assert 'href' not in result
    assert 'class' not in result


def test_strip_html_with_special_characters():
    """Test that special characters are preserved"""
    html = '<p>Special: &amp; &lt; &gt; &quot;</p>'
    result = _strip_html(html)
    assert '&' in result
    assert '<' in result or result == 'Special: & < > "'


def test_strip_html_with_whitespace():
    """Test that whitespace is handled correctly"""
    html = '<p>  Multiple   spaces  </p>'
    result = _strip_html(html)
    assert result == 'Multiple spaces'


def test_strip_html_with_line_breaks():
    """Test handling of line breaks"""
    html = '<p>Line 1<br>Line 2<br/>Line 3</p>'
    result = _strip_html(html)
    assert 'Line 1' in result
    assert 'Line 2' in result
    assert 'Line 3' in result


def test_strip_html_with_scripts_and_styles():
    """Test that script and style content is removed"""
    html = '<div><script>alert("test");</script><p>Content</p><style>.class{}</style></div>'
    result = _strip_html(html)
    assert 'alert' not in result
    assert '.class' not in result
    assert 'Content' in result


def test_strip_html_with_unicode():
    """Test handling of unicode characters"""
    html = '<p>Unicode: 中文 العربية 🎉</p>'
    result = _strip_html(html)
    assert '中文' in result
    assert 'العربية' in result
    assert '🎉' in result


# ---------- Test _first_url_from_links ----------

def test_first_url_from_links_with_none():
    """Test that None returns None"""
    assert _first_url_from_links(None) is None


def test_first_url_from_links_with_empty_list():
    """Test that empty list returns None"""
    assert _first_url_from_links([]) is None


def test_first_url_from_links_with_alternate_rel():
    """Test extracting URL with alternate rel"""
    links = [{'rel': 'alternate', 'href': 'http://example.com/alternate'}]
    result = _first_url_from_links(links)
    assert result == 'http://example.com/alternate'


def test_first_url_from_links_with_multiple_links():
    """Test that alternate rel is preferred"""
    links = [
        {'rel': 'self', 'href': 'http://example.com/self'},
        {'rel': 'alternate', 'href': 'http://example.com/alternate'},
        {'rel': 'canonical', 'href': 'http://example.com/canonical'}
    ]
    result = _first_url_from_links(links)
    assert result == 'http://example.com/alternate'


def test_first_url_from_links_without_alternate():
    """Test fallback to first link with href"""
    links = [
        {'rel': 'self', 'href': 'http://example.com/first'},
        {'rel': 'canonical', 'href': 'http://example.com/second'}
    ]
    result = _first_url_from_links(links)
    assert result == 'http://example.com/first'


def test_first_url_from_links_with_missing_href():
    """Test handling links without href"""
    links = [
        {'rel': 'alternate'},
        {'rel': 'self', 'href': 'http://example.com/found'}
    ]
    result = _first_url_from_links(links)
    assert result == 'http://example.com/found'


def test_first_url_from_links_with_whitespace():
    """Test that URLs are stripped of whitespace"""
    links = [{'rel': 'alternate', 'href': '  http://example.com/test  '}]
    result = _first_url_from_links(links)
    assert result == 'http://example.com/test'


def test_first_url_from_links_with_empty_href():
    """Test handling empty href values"""
    links = [
        {'rel': 'alternate', 'href': ''},
        {'rel': 'self', 'href': 'http://example.com/valid'}
    ]
    result = _first_url_from_links(links)
    # Should skip empty href and find valid one
    assert result is not None


# ---------- Test _best_url ----------

def test_best_url_with_link_field():
    """Test that link field is preferred"""
    entry = {'link': 'http://example.com/link'}
    result = _best_url(entry)
    assert result == 'http://example.com/link'


def test_best_url_with_links_array():
    """Test fallback to links array"""
    entry = {
        'links': [{'rel': 'alternate', 'href': 'http://example.com/alternate'}]
    }
    result = _best_url(entry)
    assert result == 'http://example.com/alternate'


def test_best_url_with_id_field():
    """Test fallback to id field when it's a URL"""
    entry = {'id': 'https://example.com/id'}
    result = _best_url(entry)
    assert result == 'https://example.com/id'


def test_best_url_with_non_url_id():
    """Test that non-URL id is ignored"""
    entry = {'id': 'some-identifier-123'}
    result = _best_url(entry)
    assert result is None


def test_best_url_with_guid_field():
    """Test fallback to guid field when it's a URL"""
    entry = {'guid': 'https://example.com/guid'}
    result = _best_url(entry)
    assert result == 'https://example.com/guid'


def test_best_url_with_non_url_guid():
    """Test that non-URL guid is ignored"""
    entry = {'guid': 'guid-12345'}
    result = _best_url(entry)
    assert result is None


def test_best_url_priority_order():
    """Test that link field takes priority over other sources"""
    entry = {
        'link': 'http://example.com/link',
        'links': [{'rel': 'alternate', 'href': 'http://example.com/alternate'}],
        'id': 'https://example.com/id',
        'guid': 'https://example.com/guid'
    }
    result = _best_url(entry)
    assert result == 'http://example.com/link'


def test_best_url_with_empty_link():
    """Test that empty link field is skipped"""
    entry = {
        'link': '   ',
        'guid': 'https://example.com/guid'
    }
    result = _best_url(entry)
    assert result == 'https://example.com/guid'


def test_best_url_with_no_valid_url():
    """Test that None is returned when no valid URL found"""
    entry = {
        'title': 'No URL',
        'id': 'non-url-id',
        'guid': 'non-url-guid'
    }
    result = _best_url(entry)
    assert result is None


def test_best_url_strips_whitespace():
    """Test that URLs are stripped of whitespace"""
    entry = {'link': '  http://example.com/test  '}
    result = _best_url(entry)
    assert result == 'http://example.com/test'


# ---------- Test _best_created ----------

def test_best_created_with_published_parsed():
    """Test that published_parsed is preferred"""
    entry = {
        'published_parsed': datetime(2025, 10, 26, 12, 0, 0).timetuple()
    }
    result = _best_created(entry)
    assert result == "2025-10-26T12:00:00Z"


def test_best_created_with_created_parsed():
    """Test fallback to created_parsed"""
    entry = {
        'created_parsed': datetime(2025, 10, 25, 10, 0, 0).timetuple()
    }
    result = _best_created(entry)
    assert result == "2025-10-25T10:00:00Z"


def test_best_created_with_updated_parsed():
    """Test fallback to updated_parsed"""
    entry = {
        'updated_parsed': datetime(2025, 10, 24, 8, 0, 0).timetuple()
    }
    result = _best_created(entry)
    assert result == "2025-10-24T08:00:00Z"


def test_best_created_priority_order():
    """Test that published_parsed takes priority"""
    entry = {
        'published_parsed': datetime(2025, 10, 26, 12, 0, 0).timetuple(),
        'created_parsed': datetime(2025, 10, 25, 10, 0, 0).timetuple(),
        'updated_parsed': datetime(2025, 10, 24, 8, 0, 0).timetuple()
    }
    result = _best_created(entry)
    assert result == "2025-10-26T12:00:00Z"


def test_best_created_with_no_dates():
    """Test that None is returned when no dates available"""
    entry = {'title': 'No dates'}
    result = _best_created(entry)
    assert result is None


# ---------- Test _best_updated ----------

def test_best_updated_with_updated_parsed():
    """Test that updated_parsed is preferred"""
    entry = {
        'updated_parsed': datetime(2025, 10, 27, 14, 0, 0).timetuple()
    }
    created_iso = "2025-10-26T12:00:00Z"
    result = _best_updated(entry, created_iso)
    assert result == "2025-10-27T14:00:00Z"


def test_best_updated_fallback_to_created():
    """Test fallback to created_iso when updated_parsed not available"""
    entry = {}
    created_iso = "2025-10-26T12:00:00Z"
    result = _best_updated(entry, created_iso)
    assert result == "2025-10-26T12:00:00Z"


def test_best_updated_with_none_created():
    """Test with None created_iso"""
    entry = {}
    result = _best_updated(entry, None)
    assert result is None


# ---------- Test _coalesce_description ----------

def test_coalesce_description_with_summary():
    """Test that summary is preferred"""
    entry = {'summary': '<p>Summary text</p>'}
    result = _coalesce_description(entry)
    assert result == 'Summary text'


def test_coalesce_description_with_description():
    """Test fallback to description field"""
    entry = {'description': '<p>Description text</p>'}
    result = _coalesce_description(entry)
    assert result == 'Description text'


def test_coalesce_description_with_content_array():
    """Test fallback to content array"""
    entry = {
        'content': [{'value': '<p>Content from array</p>'}]
    }
    result = _coalesce_description(entry)
    assert result == 'Content from array'


def test_coalesce_description_with_content_encoded():
    """Test fallback to content:encoded field"""
    entry = {'content:encoded': '<p>Encoded content</p>'}
    result = _coalesce_description(entry)
    assert result == 'Encoded content'


def test_coalesce_description_priority_order():
    """Test that summary takes priority"""
    entry = {
        'summary': '<p>Summary</p>',
        'description': '<p>Description</p>',
        'content': [{'value': '<p>Content</p>'}],
        'content:encoded': '<p>Encoded</p>'
    }
    result = _coalesce_description(entry)
    assert result == 'Summary'


def test_coalesce_description_with_empty_values():
    """Test handling of empty values"""
    entry = {
        'summary': '',
        'description': '<p>Valid description</p>'
    }
    result = _coalesce_description(entry)
    # Should return empty since summary exists but is empty
    assert result == ''


def test_coalesce_description_with_no_content():
    """Test that empty string is returned when no content found"""
    entry = {'title': 'No content'}
    result = _coalesce_description(entry)
    assert result == ''


def test_coalesce_description_with_multiple_content_items():
    """Test that first content item with value is used"""
    entry = {
        'content': [
            {'type': 'text/plain'},
            {'value': '<p>First with value</p>'},
            {'value': '<p>Second with value</p>'}
        ]
    }
    result = _coalesce_description(entry)
    assert result == 'First with value'

# ---------- normalize_all_feeds Tests ----------

def test_normalize_all_feeds_creates_output_directory(tmp_path):
    """Test that normalize_all_feeds creates the output directory if it doesn't exist"""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    
    # Create a simple XML file
    xml_content = '<?xml version="1.0"?><rss><channel><item><title>Test</title><link>http://example.com</link></item></channel></rss>'
    (input_dir / "test.xml").write_text(xml_content)
    
    # Ensure output directory doesn't exist
    assert not output_dir.exists()
    
    # Call normalize_all_feeds
    normalize_all_feeds(str(input_dir), str(output_dir))
    
    # Verify output directory was created
    assert output_dir.exists()
    assert output_dir.is_dir()

def test_normalize_all_feeds_processes_multiple_xml_files(tmp_path):
    """Test that normalize_all_feeds processes multiple XML files and creates separate normalized JSON files for each"""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    
    # Create multiple XML files
    xml_content_1 = '<?xml version="1.0"?><rss><channel><item><title>Test 1</title><link>http://example.com/1</link></item></channel></rss>'
    xml_content_2 = '<?xml version="1.0"?><rss><channel><item><title>Test 2</title><link>http://example.com/2</link></item></channel></rss>'
    xml_content_3 = '<?xml version="1.0"?><rss><channel><item><title>Test 3</title><link>http://example.com/3</link></item></channel></rss>'
    
    (input_dir / "feed1.xml").write_text(xml_content_1)
    (input_dir / "feed2.xml").write_text(xml_content_2)
    (input_dir / "feed3.xml").write_text(xml_content_3)
    
    # Call normalize_all_feeds
    output_files = normalize_all_feeds(str(input_dir), str(output_dir))
    
    # Verify 3 JSON files were created
    assert len(output_files) == 3
    assert (output_dir / "feed1.normalized.json").exists()
    assert (output_dir / "feed2.normalized.json").exists()
    assert (output_dir / "feed3.normalized.json").exists()

def test_normalize_all_feeds_saves_correct_data(tmp_path):
    """Test that normalize_all_feeds saves correct normalized data to each JSON file"""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    
    # Create XML file with specific data
    xml_content = '''<?xml version="1.0"?>
    <rss version="2.0">
        <channel>
            <item>
                <title>Sample Article</title>
                <link>http://example.com/article</link>
                <description><![CDATA[<p>This is a <b>test</b> description.</p>]]></description>
                <pubDate>Mon, 26 Oct 2025 12:00:00 GMT</pubDate>
            </item>
            <item>
                <title>Another Article</title>
                <link>http://example.com/another</link>
                <description>Plain text description</description>
            </item>
        </channel>
    </rss>'''
    
    (input_dir / "test_feed.xml").write_text(xml_content)
    
    # Call normalize_all_feeds
    output_files = normalize_all_feeds(str(input_dir), str(output_dir))
    
    # Read the generated JSON file
    assert len(output_files) == 1
    with open(output_files[0], 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Verify the data structure and content
    assert len(data) == 2
    assert data[0]['title'] == 'Sample Article'
    assert data[0]['url'] == 'http://example.com/article'
    assert 'test description' in data[0]['description']
    assert data[1]['title'] == 'Another Article'
    assert data[1]['url'] == 'http://example.com/another'
    
    # Verify all required fields are present
    for item in data:
        assert 'title' in item
        assert 'url' in item
        assert 'description' in item
        assert 'updatedDate' in item
        assert 'createdDate' in item

def test_normalize_all_feeds_handles_empty_directory(tmp_path, capsys):
    """Test that normalize_all_feeds handles an empty input directory gracefully"""
    input_dir = tmp_path / "empty_input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    
    # Call normalize_all_feeds with empty directory
    output_files = normalize_all_feeds(str(input_dir), str(output_dir))
    
    # Verify it returns an empty list
    assert output_files == []
    
    # Verify warning message was printed
    captured = capsys.readouterr()
    assert "No XML files found" in captured.out

def test_normalize_all_feeds_returns_correct_paths(tmp_path):
    """Test that normalize_all_feeds returns the correct list of paths to the generated JSON files"""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()
    
    # Create XML files
    xml_content = '<?xml version="1.0"?><rss><channel><item><title>Test</title><link>http://example.com</link></item></channel></rss>'
    (input_dir / "alpha.xml").write_text(xml_content)
    (input_dir / "beta.xml").write_text(xml_content)
    
    # Call normalize_all_feeds
    output_files = normalize_all_feeds(str(input_dir), str(output_dir))
    
    # Verify returned paths
    assert len(output_files) == 2
    assert all(os.path.exists(path) for path in output_files)
    assert any("alpha.normalized.json" in path for path in output_files)
    assert any("beta.normalized.json" in path for path in output_files)
    
    # Verify paths are absolute
    for path in output_files:
        assert os.path.isabs(path) or path.startswith(str(output_dir))
        assert path.endswith(".normalized.json")
