from transformers import pipeline, Pipeline
import logging
import traceback
from typing import Dict, Any
from dotenv import load_dotenv
import os

logging.basicConfig(
    filename="summarizer.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

MODEL_NAME = os.getenv("SUMMARIZATION_MODEL", "sshleifer/distilbart-cnn-12-6")
_summarizer: Pipeline | None = None


def summarize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Summarize the 'scraped_content' field in a JSON record.
    Returns a dict with title, summary, and error info (if any).
    """
    global _summarizer

    try:
        if not isinstance(record, dict):
            logging.error(f"Invalid input type: {type(record)}. Expected dict.")
            return {"error": "Invalid record type", "summary": "", "title": "Unknown"}

        title = record.get("title", "Untitled")
        desc = record.get("scraped_content", "")

        if not desc or not isinstance(desc, str) or not desc.strip():
            logging.warning(f"Skipping record '{title}' — empty or invalid content.")
            return {"title": title, "summary": "", "error": "Empty or invalid content"}

        if _summarizer is None:
            try:
                logging.info(f"Initializing summarizer with model: {MODEL_NAME}")
                _summarizer = pipeline("summarization", model=MODEL_NAME)
            except Exception as e:
                logging.exception(f"Failed to initialize summarization model: {e}")
                return {"title": title, "summary": "", "error": f"Model init failed: {str(e)}"}

        logging.info(f"Summarizing record: '{title}' (length={len(desc)} chars)")

        max_chunk = 1000
        chunks = [desc[i:i + max_chunk] for i in range(0, len(desc), max_chunk)]
        summaries = []

        for i, chunk in enumerate(chunks):
            logging.debug(f"Processing chunk {i+1}/{len(chunks)} for '{title}'")
            try:
                dynamic_max_len = min(200, max(20, len(chunk) // 2))
                summary = _summarizer(
                    chunk,
                    max_length=dynamic_max_len,
                    min_length=10,
                    do_sample=False
                )
                summaries.append(summary[0]["summary_text"])
            except Exception as e:
                tb = traceback.format_exc()
                logging.error(f"Error summarizing chunk {i+1}/{len(chunks)} for '{title}': {e}\n{tb}")
                summaries.append(f"[Error in chunk {i+1}: {str(e)}]")

        final_summary = " ".join(summaries).strip()
        logging.info(
            f"Completed summary for '{title}' — summary length={len(final_summary)} chars"
        )

        return {
            "title": title,
            "summary": final_summary,
            "url": record.get("url", ""),
            "scraped_at": record.get("scraped_at", ""),
            "word_count": record.get("word_count", 0),
            "metadata": record.get("scraped_metadata", {}),
            "error": None
        }

    except Exception as e:
        tb = traceback.format_exc()
        logging.critical(f"Unexpected failure while summarizing record: {e}\n{tb}")
        return {
            "title": record.get("title", "Unknown") if isinstance(record, dict) else "Unknown",
            "summary": "",
            "error": f"Unexpected error: {str(e)}"
        }
