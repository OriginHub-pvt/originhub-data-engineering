import pytest
from datetime import datetime
from dags.src.xml_to_json import _to_iso_from_struct, _strip_html, normalize_entry, parse_xml_file, dedupe_by_url
import logging

# Sample data for testing
sample_entry = {
    'title': 'Sample Entry',
    'link': 'http://example.com',
    'summary': '<p>This is a <b>sample</b> summary.</p>',
    'published_parsed': datetime(2025, 10, 26, 0, 0).timetuple(),
    'created_parsed': None,
    'updated_parsed': None,
}

# ---------- Utility Function Tests ----------

def test_to_iso_from_struct():
    assert _to_iso_from_struct(None) is None
    assert _to_iso_from_struct(datetime(2025, 10, 26, 12, 0, 0).timetuple()) == "2025-10-26T12:00:00Z"

def test_strip_html():
    assert _strip_html('<p>Test</p>') == 'Test'
    assert _strip_html(None) == ''

# Additional Test for _to_iso_from_struct with malformed structure
def test_to_iso_from_struct_malformed():
    assert _to_iso_from_struct(object()) is None

# Additional edge case tests for _strip_html
def test_strip_html_complex():
    assert _strip_html('<p><b>Bold</b> and <a href="#">link</a></p>') == 'Bold and link'

# ---------- Main Function Tests ----------

def test_normalize_entry():
    result = normalize_entry(sample_entry)
    assert result['title'] == 'Sample Entry'
    assert result['url'] == 'http://example.com'
    assert result['description'] == 'This is a sample summary.'
    assert result['updatedDate'] == '2025-10-26T00:00:00Z'
    assert result['createdDate'] == '2025-10-26T00:00:00Z'

def test_parse_xml_file(tmp_path):
    xml_content = '<?xml version="1.0"?><rss><channel><item><title>Example</title></item></channel></rss>'
    xml_file = tmp_path / "test.xml"
    xml_file.write_text(xml_content)
    parsed = parse_xml_file(str(xml_file))
    assert len(parsed) == 1
    assert parsed[0]['title'] == 'Example'

def test_dedupe_by_url():
    items = [
        {'url': 'http://example.com', 'title': 'Example One', 'createdDate': '2025-10-26'},
        {'url': 'http://example.com', 'title': 'Example Two', 'createdDate': '2025-10-26'},
    ]
    deduped = dedupe_by_url(items)
    assert len(deduped) == 1

# Test parse_xml_file with malformed XML content
def test_parse_xml_file_malformed(tmp_path, caplog):
    malformed_content = '<?xml version="1.0"?><rss><channel><item><title>Missing closing tags'
    xml_file = tmp_path / "malformed.xml"
    xml_file.write_text(malformed_content)
    
    with caplog.at_level(logging.ERROR):
        result = parse_xml_file(str(xml_file))
        assert isinstance(result, list)

# Comprehensive test from parsing to deduplication
def test_full_workflow(tmp_path):
    complete_content = '<?xml version="1.0"?><rss><channel><item><title>Complete Test</title><link>http://example.com/test</link><description>Full processing test</description></item></channel></rss>'
    xml_file = tmp_path / "complete.xml"
    xml_file.write_text(complete_content)
    
    parsed = parse_xml_file(str(xml_file))
    deduped = dedupe_by_url(parsed)
    assert len(deduped) == 1
    assert deduped[0]['title'] == 'Complete Test'

# Test additional edge cases
def test_normalize_entry_with_missing_fields():
    """Test normalize_entry with minimal data"""
    minimal_entry = {'title': 'Minimal'}
    result = normalize_entry(minimal_entry)
    assert result['title'] == 'Minimal'
    assert result['url'] == ''
    assert result['description'] == ''
    assert result['updatedDate'] == ''
    assert result['createdDate'] == ''

def test_normalize_entry_with_alternative_url_sources():
    """Test that _best_url checks alternative URL sources"""
    entry_with_links = {
        'title': 'Test',
        'links': [{'rel': 'alternate', 'href': 'http://example.com/alternate'}]
    }
    result = normalize_entry(entry_with_links)
    assert result['url'] == 'http://example.com/alternate'

def test_dedupe_by_url_with_no_url():
    """Test deduplication when items have no URL"""
    items = [
        {'title': 'Test 1', 'createdDate': '2025-10-26'},
        {'title': 'Test 2', 'createdDate': '2025-10-26'},  # Different title
        {'title': 'test 1', 'createdDate': '2025-10-26'},  # Same as first (case insensitive)
    ]
    deduped = dedupe_by_url(items)
    # First two should be unique, third should be deduplicated
    assert len(deduped) == 2

def test_coalesce_description_from_content():
    """Test that description is extracted from content array"""
    entry_with_content = {
        'title': 'Test',
        'content': [{'value': '<p>Content from array</p>'}]
    }
    result = normalize_entry(entry_with_content)
    assert result['description'] == 'Content from array'

def test_best_url_with_guid():
    """Test URL extraction from guid field"""
    entry_with_guid = {
        'title': 'Test',
        'guid': 'https://example.com/guid'
    }
    result = normalize_entry(entry_with_guid)
    assert result['url'] == 'https://example.com/guid'
