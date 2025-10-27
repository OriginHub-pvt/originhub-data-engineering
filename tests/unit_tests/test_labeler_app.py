import io
import json
import pytest
import pandas as pd
from labeling.labeler_app import JsonLabelingApp

@pytest.fixture
def sample_data():
    """Normal dataset with all expected fields."""
    return [
        {
            "title": "Top 10 AI Tools That Will Transform Your Content Creation in 2025",
            "url": "https://techncruncher.blogspot.com/2025/01/top-10-ai-tools-that-will-transform.html",
            "description": "Looking to level up your content creation game in 2025?",
            "createdDate": "2025-01-02T09:26:00Z",
            "updatedDate": "2025-01-02T09:45:03Z"
        },
        {
            "title": "Understanding Reinforcement Learning",
            "url": "https://example.com/rl",
            "description": "Reinforcement learning overview",
            "createdDate": "2025-01-03T10:00:00Z",
            "updatedDate": "2025-01-03T10:15:00Z"
        }
    ]

@pytest.fixture(autouse=True)
def setup_streamlit(monkeypatch):
    """Provide dummy Streamlit session state that supports both key and attribute access."""
    import streamlit as st

    class DummySession(dict):
        def __getattr__(self, name):
            try:
                return self[name]
            except KeyError:
                raise AttributeError(f"'DummySession' object has no attribute '{name}'")

        def __setattr__(self, name, value):
            self[name] = value

        def update(self, **kwargs):
            for k, v in kwargs.items():
                self[k] = v

    # Initialize dummy state
    st.session_state = DummySession(index=0, labels=[], expanded=False, saved=False)
    yield
    st.session_state.clear()

def test_load_json_valid(sample_data):
    """Valid JSON list should load and initialize session labels."""
    instance = JsonLabelingApp.__new__(JsonLabelingApp)
    instance._setup_session_state()
    json_stream = io.StringIO(json.dumps(sample_data))

    instance.load_json(json_stream)

    assert isinstance(instance.data, list)
    assert len(instance.data) == 2
    import streamlit as st
    assert st.session_state.labels == [None, None]
    assert "createdDate" in instance.data[0] and "updatedDate" in instance.data[0]

def test_load_json_invalid(monkeypatch):
    """Non-list JSON should trigger Streamlit error and stop."""
    import streamlit as st
    instance = JsonLabelingApp.__new__(JsonLabelingApp)
    instance._setup_session_state()

    called = {}

    def fake_error(msg): called["error"] = msg
    def fake_stop(): called["stop"] = True; raise SystemExit

    monkeypatch.setattr(st, "error", fake_error)
    monkeypatch.setattr(st, "stop", fake_stop)

    invalid_stream = io.StringIO(json.dumps({"not": "a list"}))
    with pytest.raises(SystemExit):
        instance.load_json(invalid_stream)

    assert "error" in called and "stop" in called

def test_label_and_next(sample_data):
    """Labeling an entry should record label and move to next index."""
    import streamlit as st
    instance = JsonLabelingApp.__new__(JsonLabelingApp)
    instance.data = sample_data
    instance._setup_session_state()
    st.session_state.labels = [None] * len(sample_data)

    # Label first entry
    st.session_state.index = 0
    instance._label_and_next(1)
    assert st.session_state.labels[0] == 1
    assert st.session_state.index == 1

    # Label last entry (should not go out of range)
    instance._label_and_next(0)
    assert st.session_state.labels[1] == 0
    assert st.session_state.index == 1  # stays at last


def test_label_out_of_bounds(monkeypatch):
    """Gracefully handle empty data when labeling."""
    import streamlit as st
    instance = JsonLabelingApp.__new__(JsonLabelingApp)
    instance.data = []
    instance._setup_session_state()
    st.session_state.index = 0
    st.session_state.labels = []
    # Should not crash even if index > data length
    try:
        instance._label_and_next(1)
    except Exception as e:
        pytest.fail(f"Labeling raised an exception on empty data: {e}")

def test_generate_json_bytes_preserves_fields(sample_data):
    """Generated JSON bytes should preserve all keys."""
    instance = JsonLabelingApp.__new__(JsonLabelingApp)
    for item in sample_data:
        item["label"] = 1
    json_bytes = instance._generate_json_bytes(sample_data)
    decoded = json.loads(json_bytes.decode("utf-8"))
    assert all(k in decoded[0] for k in ["title", "url", "description", "label"])
    assert isinstance(json_bytes, bytes)

def test_generate_csv_bytes_preserves_fields(sample_data):
    """Generated CSV should include all expected columns."""
    instance = JsonLabelingApp.__new__(JsonLabelingApp)
    for item in sample_data:
        item["label"] = 1
    csv_bytes = instance._generate_csv_bytes(sample_data)
    df = pd.read_csv(io.BytesIO(csv_bytes))
    for col in ["title", "url", "description", "label", "createdDate", "updatedDate"]:
        assert col in df.columns
    assert len(df) == len(sample_data)

def test_empty_json_list(monkeypatch):
    """Empty JSON list should initialize with empty labels."""
    import streamlit as st
    instance = JsonLabelingApp.__new__(JsonLabelingApp)
    instance._setup_session_state()
    json_stream = io.StringIO("[]")
    instance.load_json(json_stream)
    assert instance.data == []
    assert st.session_state.labels == []


def test_missing_optional_fields(monkeypatch):
    """JSON missing optional fields should still load."""
    import streamlit as st
    data = [{"title": "Test Only"}]
    instance = JsonLabelingApp.__new__(JsonLabelingApp)
    instance._setup_session_state()
    json_stream = io.StringIO(json.dumps(data))
    instance.load_json(json_stream)
    assert "title" in instance.data[0]
    assert st.session_state.labels == [None]


def test_short_description_preview(monkeypatch):
    """Descriptions <100 chars should not error out."""
    instance = JsonLabelingApp.__new__(JsonLabelingApp)
    short_item = [{
        "title": "Short Desc",
        "url": "https://example.com",
        "description": "Short",
        "createdDate": "2025-01-01T00:00:00Z",
        "updatedDate": "2025-01-01T01:00:00Z"
    }]
    instance.data = short_item
    instance._setup_session_state()
    try:
        # We only care that it doesn't raise
        instance._generate_json_bytes(short_item)
    except Exception as e:
        pytest.fail(f"Short description caused error: {e}")


def test_non_utf8_content(monkeypatch):
    """Ensure non-ASCII chars are correctly handled."""
    instance = JsonLabelingApp.__new__(JsonLabelingApp)
    data = [{
        "title": "Café AI",
        "url": "https://example.com",
        "description": "Résumé and naïve analysis.",
        "createdDate": "2025-01-02T00:00:00Z",
        "updatedDate": "2025-01-02T01:00:00Z",
        "label": 1
    }]
    json_bytes = instance._generate_json_bytes(data)
    decoded = json.loads(json_bytes.decode("utf-8"))
    assert decoded[0]["title"] == "Café AI"
    csv_bytes = instance._generate_csv_bytes(data)
    df = pd.read_csv(io.BytesIO(csv_bytes))
    assert "Café" in df.iloc[0]["title"]

def test_end_to_end_labeling_and_export(tmp_path, sample_data):
    """
    Simulate full app flow:
    1. Load JSON
    2. Label entries
    3. Generate downloadable JSON + CSV
    4. Validate outputs
    """
    import streamlit as st
    instance = JsonLabelingApp.__new__(JsonLabelingApp)
    instance._setup_session_state()

    # Step 1: Load JSON
    json_stream = io.StringIO(json.dumps(sample_data))
    instance.load_json(json_stream)

    # Step 2: Label both items
    st.session_state.labels = [1, 0]

    # Step 3: Inject labels into data and export
    for i, label in enumerate(st.session_state.labels):
        instance.data[i]["label"] = label

    json_bytes = instance._generate_json_bytes(instance.data)
    csv_bytes = instance._generate_csv_bytes(instance.data)

    # Step 4: Save temporary files and re-read
    json_path = tmp_path / "output.json"
    csv_path = tmp_path / "output.csv"

    json_path.write_bytes(json_bytes)
    csv_path.write_bytes(csv_bytes)

    with open(json_path, "r", encoding="utf-8") as f:
        saved_json = json.load(f)
    saved_csv = pd.read_csv(csv_path)

    # Validate integrity
    assert len(saved_json) == 2
    assert saved_json[0]["label"] == 1
    assert "createdDate" in saved_json[0]
    assert all(c in saved_csv.columns for c in ["title", "url", "description", "label"])
    assert not saved_csv.isnull().any().any()
