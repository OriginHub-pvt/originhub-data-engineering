from transformers import pipeline
import json
import io

MODEL_NAME = "google/pegasus-cnn_dailymail"
_summarizer = None  # cached global


def summarize_record(record):
    """Summarize the 'content' field in a JSON record."""
    global _summarizer
    if _summarizer is None:
        _summarizer = pipeline("summarization", model=MODEL_NAME)

    desc = record.get("content", "")
    if not desc.strip():
        return {"title": record.get("title", ""), "summary": ""}

    max_chunk = 1000
    chunks = [desc[i:i + max_chunk] for i in range(0, len(desc), max_chunk)]
    summaries = []
    for chunk in chunks:
        summary = _summarizer(chunk, max_length=200, min_length=50, do_sample=False)
        summaries.append(summary[0]["summary_text"])

    return {
        "title": record.get("title", ""),
        "summary": " ".join(summaries),
    }