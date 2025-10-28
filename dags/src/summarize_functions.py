from transformers import pipeline
import json
import io

MODEL_NAME = "google/pegasus-cnn_dailymail"

summarizer = pipeline("summarization", model=MODEL_NAME)

def summarize_record(record):
    """Summarize the 'description' field in a JSON record."""
    desc = record.get("description", "")
    if not desc.strip():
        record["summary"] = ""
        return record

    max_chunk = 1000
    chunks = [desc[i:i+max_chunk] for i in range(0, len(desc), max_chunk)]
    summaries = []
    for chunk in chunks:
        summary = summarizer(chunk, max_length=200, min_length=50, do_sample=False)
        summaries.append(summary[0]['summary_text'])
    record["summary"] = " ".join(summaries)
    return record


def serialize_json(data):
    """Convert list of dicts to a byte stream for GCS upload."""
    return io.BytesIO(json.dumps(data, indent=2).encode("utf-8"))