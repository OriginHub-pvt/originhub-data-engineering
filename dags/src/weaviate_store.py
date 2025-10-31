import os
import logging
from dotenv import load_dotenv
import weaviate
from weaviate.classes.config import Configure

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

load_dotenv()

WEAVIATE_HOST = os.getenv("WEAVIATE_HOST", "weaviate")
WEAVIATE_PORT = int(os.getenv("WEAVIATE_PORT", 8080))
WEAVIATE_GRPC_PORT = int(os.getenv("WEAVIATE_GRPC_PORT", 50051))
WEAVIATE_MODEL = os.getenv("WEAVIATE_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
WEAVIATE_COLLECTION = os.getenv("WEAVIATE_COLLECTION", "ArticleSummary")

logger.info(
    f"Config loaded → host={WEAVIATE_HOST}, port={WEAVIATE_PORT}, grpc={WEAVIATE_GRPC_PORT}, "
    f"model={WEAVIATE_MODEL}, collection={WEAVIATE_COLLECTION}"
)

try:
    client = weaviate.connect_to_local(
        host=WEAVIATE_HOST,
        port=WEAVIATE_PORT,
        grpc_port=WEAVIATE_GRPC_PORT,
    )
    logger.info("Connected to Weaviate successfully.")
except Exception as e:
    logger.error(f"Failed to connect to Weaviate: {e}")
    raise

def ensure_collection(name: str):
    """
    Ensure a collection exists in Weaviate; create if missing.
    """
    try:
        existing = [c.name for c in client.collections.list_all()]
        if name not in existing:
            client.collections.create(
                name=name,
                vector_config=Configure.Vectors.text2vec_huggingface(model=WEAVIATE_MODEL),
            )
            logger.info(f"Created new collection '{name}' using model '{WEAVIATE_MODEL}'.")
        else:
            logger.debug(f"Collection '{name}' already exists.")
        return client.collections.get(name)
    except Exception as e:
        logger.error(f"Failed to ensure collection '{name}': {e}")
        raise

def store_summary(record: dict) -> str:
    """
    Store a summarized article (title + summary) in Weaviate.
    Automatically creates the collection if it doesn't exist.
    """
    try:
        title = record.get("title", "Untitled")
        summary = record.get("summary", "")

        if not summary.strip():
            logger.warning(f"Skipping record '{title}' — empty summary.")
            return ""

        # Ensure collection exists
        collection = ensure_collection(WEAVIATE_COLLECTION)

        # Insert record
        uuid = collection.data.insert(properties={"title": title, "summary": summary})
        logger.info(f"Stored summary for '{title}' (UUID: {uuid}) in '{WEAVIATE_COLLECTION}'.")
        return str(uuid)
    except Exception as e:
        logger.error(f"Failed to store summary for '{record.get('title', 'Unknown')}': {e}")
        raise


def fetch_summary(object_id: str):
    """
    Fetch and print stored article summary by object ID.
    """
    try:
        collection = ensure_collection(WEAVIATE_COLLECTION)
        obj = collection.query.fetch_object_by_id(object_id, include_vector=False)
        if obj:
            logger.info(f"Fetched from '{WEAVIATE_COLLECTION}': {obj.properties}")
            return obj.properties
        else:
            logger.warning(f"No object found with ID {object_id}")
            return None
    except Exception as e:
        logger.error(f"Error fetching object {object_id}: {e}")
        raise


def delete_collection():
    """
    Delete the configured collection from Weaviate.
    """
    try:
        client.collections.delete(WEAVIATE_COLLECTION)
        logger.info(f"Deleted collection '{WEAVIATE_COLLECTION}'.")
    except Exception as e:
        logger.error(f"Failed to delete collection '{WEAVIATE_COLLECTION}': {e}")


if __name__ == "__main__":
    try:
        # Step 1: Auto-create collection
        ensure_collection(WEAVIATE_COLLECTION)

        # Step 2: Insert a sample record
        sample = {
            "title": "AI and the Future of Work",
            "summary": "Artificial Intelligence is transforming the workforce by automating repetitive tasks."
        }
        obj_id = store_summary(sample)

        # Step 3: Fetch to verify
        if obj_id:
            fetched = fetch_summary(obj_id)
            print("\nRetrieved from Weaviate:", fetched)

        # Step 4: Optional cleanup
        # delete_collection()

    finally:
        client.close()
        logger.info("Weaviate connection closed.")
