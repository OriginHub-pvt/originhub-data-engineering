# JSON Labeling Tool (Streamlit)

A simple Streamlit-based app for manual labeling of JSON items.

## What this does

The app `labeler_app.py` provides a UI to:

- Upload a JSON file containing a list of items (each item is a dict/object).
- View each item's `title`, `url`, and `description` (preview + expand).
- Mark each item as Relevant (1) or Irrelevant (0).
- Navigate between entries.
- Save labels into the dataset and download the labeled dataset as JSON or CSV.

## Requirements

Dependencies for the labeling app are listed in `labeling/requirements.txt`. The key dependencies are:

- streamlit
- pandas

## Install (recommended: virtualenv)

From the `labeling` directory or project root, create and activate a virtual environment, then install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r labeling/requirements.txt
```

If you prefer system-wide installation, you can also run:

```bash
pip install -r labeling/requirements.txt
```

## Run the app

Start the Streamlit app from the `labeling` directory (or specify full path):

```bash
cd labeling
streamlit run labeler_app.py
```

This should open a web UI (or provide a local URL like `http://localhost:8501`) where you can upload your JSON file and start labeling.

## Expected JSON input format

The app expects the uploaded file to contain a JSON array (list) of objects. Each object MUST contain the following keys (extra keys are allowed):

- `title` (string) — the title of the item
- `url` (string) — the item's URL
- `description` (string) — the text content shown for labeling
- `createdDate` (string, ISO 8601) — creation timestamp, e.g. `2025-10-01T12:00:00Z`
- `updatedDate` (string, ISO 8601) — last updated timestamp, e.g. `2025-10-10T12:00:00Z`

All five keys are expected for each item. If a key is missing the app may still display the item, but downstream processing and CSV output will assume these fields exist; it's best to include them in the input JSON to avoid missing columns or unexpected nulls.

Example minimal JSON (an array of objects):

```json
[
  {
    "title": "Example item 1",
    "url": "https://example.com/1",
    "createdDate": "2025-10-01T12:00:00Z",
    "updatedDate": "2025-10-10T12:00:00Z",
    "description": "This is a description for the first item."
  },
  {
    "title": "Example item 2",
    "url": "https://example.com/2",
    "createdDate": "2025-09-20T08:30:00Z",
    "updatedDate": "2025-10-05T09:00:00Z",
    "description": "Second item long description..."
  }
]
```

Save the file as e.g. `input_data.json` and upload it in the Streamlit UI.

## How to label

1. Upload your JSON file via the "Upload your JSON file" control.
2. For each entry, you will see:
   - `Title` and `URL`
   - `Description` — shown truncated (100 chars) with a "Show more" button to expand.
3. Click the labeling buttons:
   - `✅ Relevant (1)` — marks the current entry as relevant.
   - `❌ Irrelevant (0)` — marks it as irrelevant.
4. Use the `⬅️ Previous` and `➡️ Next` buttons to navigate entries.

Notes:

- Labels are stored in the session while you use the app. Use the "💾 Save Labels" button to write the `label` field into each object in memory.
- After saving, you can download two files from the UI:
  - `labeled_output.json` — the original objects with a new `label` field.
  - `labeled_output.csv` — a CSV representation (flattened) including the `label` column.

## Output format

When you click "💾 Save Labels", the app adds a `label` key to each item in the loaded JSON array. The values will be `1`, `0`, or `null` (if unlabeled). Example:

```json
{
  "title": "Example item 1",
  "url": "https://example.com/1",
  "description": "...",
  "createdDate": "2025-09-20T08:30:00Z",
  "updatedDate": "2025-10-05T09:00:00Z",
  "label": 1
}
```

The downloadable CSV will contain columns for the top-level fields (e.g., `title`, `url`, `description`, `createdDate`, `updatedDate`, `label`). If your objects have nested structures, consider pre-processing them into a flattened list before using this app.

## Tests & verification

There are unit tests in `tests/unit_tests/test_labeler_app.py`. Run test suite using your preferred test runner (project uses pytest in other parts). Example:

```bash
# from repo root (activate venv first)
pytest -q
```

(If you don't have pytest installed globally, add it to your environment with `pip install pytest`.)
