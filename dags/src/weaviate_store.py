import os
import logging
import weaviate

from dotenv import load_dotenv

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

load_dotenv()

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def get_client():
    """
    Create and return a configured Weaviate client using environment variables.
    """
    weaviate_url = os.getenv("WEAVIATE_URL", "http://localhost:8081")

    logger.info(f"Connecting to Weaviate at {weaviate_url}")

    try:
        client = weaviate.Client(url=weaviate_url)

        if not client.is_ready():
            logger.warning("Weaviate client created, but server not ready!")
        else:
            logger.info("Connected to Weaviate successfully.")
        return client

    except Exception as e:
        logger.error(f"Failed to connect to Weaviate: {e}")
        raise


def ensure_schema(client):
    """
    Ensure that the 'ArticleSummary' schema exists in Weaviate.
    If not, create it with text2vec-transformers vectorizer.
    """
    class_name = "ArticleSummary"

    try:
        if not client.schema.exists(class_name):
            schema = {
                "class": class_name,
                "description": "Summarized RSS articles stored from OriginHub pipeline.",
                "vectorizer": "text2vec-transformers",
                "moduleConfig": {
                    "text2vec-transformers": {
                        "poolingStrategy": "masked_mean",
                        "vectorizeClassName": False
                    }
                },
                "properties": [
                    {"name": "title", "dataType": ["text"]},
                    {"name": "summary", "dataType": ["text"]},
                ],
            }
            client.schema.create_class(schema)
            logger.info("Created 'ArticleSummary' schema in Weaviate.")
        else:
            logger.debug("'ArticleSummary' schema already exists.")
    except Exception as e:
        logger.error(f"Failed to ensure schema: {e}")
        raise

def store_in_weaviate(record: dict):
    """
    Store summarized record in Weaviate with automatic vectorization.
    Assumes Weaviate is running locally with text2vec-transformers.
    """
    client = get_client()
    ensure_schema(client)

    title = record.get("title", "")
    summary = record.get("summary", "")

    data = {
        "title": title,
        "summary": summary,
    }

    try:
        client.data_object.create(data_object=data, class_name="ArticleSummary")
        logger.info(f"Stored article in Weaviate: '{title or 'Untitled'}'")
    except Exception as e:
        logger.error(f"Failed to store article '{title}': {e}")
        raise