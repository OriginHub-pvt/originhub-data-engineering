import argparse
import json
import logging
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import torch
from sklearn.metrics import accuracy_score, precision_recall_fscore_support
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from dotenv import load_dotenv
from google.cloud import storage 
from google.oauth2 import service_account


LOGGER = logging.getLogger("bias_detection")
load_dotenv()

DEFAULT_SLICE_COLS = (
    "ai_group",
    "source_type",
    "region",
    "article_type",
    "org_type",
    "length_bucket",
)

MODEL_BUCKET = os.environ.get("MODEL_BUCKET")
MODEL_NAME = os.environ.get("MODEL_NAME")
MODEL_VERSION = os.environ.get("MODEL_VERSION")
DATA_BUCKET = os.environ.get("DATA_BUCKET", "filtering_model_training_data")
BIAS_DATA_FOLDER = os.environ.get("DATA_FOLDER", "bias_detection")
BIAS_DATA_FILE = os.environ.get("DATA_FILE", "bias_detection_labeled_dataset.csv")

TOKENIZER_NAME = "distilbert-base-uncased"

GCP_CREDENTIALS_ENV = (
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    or os.environ.get("GCP_CREDENTIALS_PATH")
)

def build_model_uri_from_env() -> Optional[str]:
    """Compute gs:// URI from env vars when explicit MODEL_URI not provided."""
    return f"gs://{MODEL_BUCKET}/{MODEL_NAME}/v{MODEL_VERSION}/"


def default_model_cache_dir() -> Path:
    """Mirror filter_articles temp path logic when env provides model metadata."""
    cache_hint = os.environ.get("MODEL_TMP_PATH")
    if cache_hint:
        return Path(cache_hint)
    if MODEL_NAME and MODEL_VERSION:
        return Path("/tmp") / f"{MODEL_NAME}_v{MODEL_VERSION}"
    return Path("Bias_Detection/model_cache")


def build_data_uri_from_env() -> Optional[str]:
    """Construct dataset URI from env defaults when explicit DATASET_URI missing."""
    return f"gs://{DATA_BUCKET}/{BIAS_DATA_FOLDER}/{BIAS_DATA_FILE}"


def resolve_credentials_path(explicit: Optional[str] = None) -> Optional[Path]:
    """Locate a service account file from CLI/env defaults."""
    candidates = [
        explicit,
        GCP_CREDENTIALS_ENV,
        "gcp.json",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        candidate_path = Path(candidate).expanduser()
        if candidate_path.exists():
            return candidate_path
    return None


def is_gcs_uri(path: str) -> bool:
    """Return True if the provided path points to a GCS URI."""
    return path.startswith("gs://")


def parse_gcs_uri(uri: str) -> Tuple[str, str]:
    """Split a gs:// URI into bucket and object path."""
    if not is_gcs_uri(uri):
        raise ValueError(f"Expected a gs:// URI, received: {uri}")
    without_scheme = uri.replace("gs://", "", 1)
    bucket, _, blob = without_scheme.partition("/")
    if not bucket or not blob:
        raise ValueError(f"Invalid gs:// URI: {uri}")
    return bucket, blob


class GCSArtifactFetcher:
    """Download artifacts from Google Cloud Storage."""

    def __init__(
        self,
        project_id: Optional[str] = None,
        credentials_path: Optional[Path] = None,
    ) -> None:
        if storage is None:
            raise ImportError(
                "google-cloud-storage is required to download artifacts from GCS. "
                "Install google-cloud-storage or provide local paths instead."
            )

        self.credentials = self._load_credentials(credentials_path)
        self.client = storage.Client(project=project_id, credentials=self.credentials)

    @staticmethod
    def _load_credentials(path: Optional[Path]) -> Optional["service_account.Credentials"]:
        """Load service account credentials from the provided path if possible."""
        if not path:
            return None
        if service_account is None:
            raise ImportError(
                "google-auth is required to use service account credentials. Install google-auth."
            )
        return service_account.Credentials.from_service_account_file(str(path))

    def download_file(self, uri: str, destination: Path) -> Path:
        """Download a single object to the desired local path."""
        bucket_name, blob_name = parse_gcs_uri(uri)
        destination.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Downloading %s -> %s", uri, destination)

        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            raise FileNotFoundError(f"{uri} does not exist in bucket {bucket_name}")
        blob.download_to_filename(str(destination))
        return destination

    def download_artifact(self, uri: str, destination_root: Path) -> Path:
        """
        Download a directory (model artifacts) from GCS.

        The returned Path points to the root directory containing Hugging Face files.
        """
        bucket_name, prefix = parse_gcs_uri(uri.rstrip("/"))
        LOGGER.info("Downloading artifact directory %s to %s", uri, destination_root)

        blobs = list(self.client.list_blobs(bucket_name, prefix=prefix))
        if not blobs:
            raise FileNotFoundError(f"No objects found under {uri}")

        destination_root.mkdir(parents=True, exist_ok=True)

        for blob in blobs:
            # Keep only the file name at the end (mirror your existing logic)
            local_path = destination_root / Path(blob.name).name
            local_path.parent.mkdir(parents=True, exist_ok=True)
            LOGGER.debug("Downloading %s -> %s", blob.name, local_path)
            blob.download_to_filename(str(local_path))

        return destination_root


@dataclass
class SliceMetrics:
    """Container to serialize per-slice evaluation metrics."""

    slice_name: str
    slice_value: str
    count: int
    accuracy: float
    precision: float
    recall: float
    f1: float


class BiasEvaluator:
    """Loads the fine-tuned transformer and evaluates it over metadata slices."""

    def __init__(
        self,
        model_dir: Path,
        text_fields: Sequence[str] = ("title", "description"),
        max_length: int = 512,
        batch_size: int = 16,
        tokenizer_name: Optional[str] = None,
    ) -> None:
        self.model_dir = model_dir
        self.text_fields = text_fields
        self.max_length = max_length
        self.batch_size = batch_size
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        LOGGER.info("Loading tokenizer and model from %s", model_dir)
        tokenizer_source = tokenizer_name or model_dir
        self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_source)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_dir)
        self.model.to(self.device)
        self.model.eval()

    @torch.no_grad()
    def predict(self, df: pd.DataFrame) -> Tuple[List[int], List[float]]:
        """Run transformer inference and return predictions + confidence."""
        texts = (
            df[list(self.text_fields)]
            .fillna("")
            .agg(lambda row: " ".join(part for part in row if part), axis=1)
            .tolist()
        )

        predictions: List[int] = []
        confidences: List[float] = []

        for start in range(0, len(texts), self.batch_size):
            batch_texts = texts[start : start + self.batch_size]
            encoded = self.tokenizer(
                batch_texts,
                padding=True,
                truncation=True,
                return_tensors="pt",
                max_length=self.max_length,
            )
            encoded = {k: v.to(self.device) for k, v in encoded.items()}
            logits = self.model(**encoded).logits
            probs = torch.softmax(logits, dim=-1)
            batch_preds = torch.argmax(probs, dim=-1)

            predictions.extend(batch_preds.cpu().tolist())
            confidences.extend(probs.max(dim=-1).values.cpu().tolist())

        return predictions, confidences

    @staticmethod
    def compute_classification_metrics(y_true: Iterable[int], y_pred: Iterable[int]) -> Dict[str, float]:
        """Calculate standard binary classification metrics."""
        y_true_list = list(y_true)
        y_pred_list = list(y_pred)
        precision, recall, f1, _ = precision_recall_fscore_support(
            y_true_list, y_pred_list, average="binary", zero_division=0
        )
        return {
            "accuracy": accuracy_score(y_true_list, y_pred_list),
            "precision": precision,
            "recall": recall,
            "f1": f1,
        }

    def evaluate_slices(
        self,
        df: pd.DataFrame,
        y_true: Sequence[int],
        y_pred: Sequence[int],
        slice_columns: Sequence[str],
    ) -> Dict[str, List[SliceMetrics]]:
        """Compute metrics for each requested metadata column."""
        results: Dict[str, List[SliceMetrics]] = {}
        joined = df.copy()
        joined["__y_true__"] = y_true
        joined["__y_pred__"] = y_pred

        for column in slice_columns:
            if column not in joined.columns:
                LOGGER.warning("Skipping missing slice column: %s", column)
                continue

            slice_rows: List[SliceMetrics] = []
            for value, group in joined.groupby(column):
                support = len(group)
                if support == 0:
                    continue
                metrics = self.compute_classification_metrics(group["__y_true__"], group["__y_pred__"])
                slice_rows.append(
                    SliceMetrics(
                        slice_name=column,
                        slice_value=str(value),
                        count=support,
                        accuracy=metrics["accuracy"],
                        precision=metrics["precision"],
                        recall=metrics["recall"],
                        f1=metrics["f1"],
                    )
                )

            if slice_rows:
                # Sort in descending order of F1 to highlight top performing group first
                results[column] = sorted(slice_rows, key=lambda item: item.f1, reverse=True)

        return results


def load_dataframe(data_path: Path) -> pd.DataFrame:
    """Read the labeled CSV dataset."""
    LOGGER.info("Loading dataset from %s", data_path)
    df = pd.read_csv(data_path)
    if "label" not in df.columns:
        raise ValueError("Dataset must include a 'label' column.")
    df = df.dropna(subset=["label"]).copy()
    df["label"] = df["label"].astype(int)
    return df


def summarize_disparities(slice_metrics: Dict[str, List[SliceMetrics]]) -> Dict[str, float]:
    """Compute the largest F1 disparity for each slice column."""
    disparities: Dict[str, float] = {}
    for slice_name, rows in slice_metrics.items():
        if len(rows) < 2:
            continue
        f1_scores = [row.f1 for row in rows]
        disparities[slice_name] = max(f1_scores) - min(f1_scores)
    return disparities


def write_metrics(
    output_dir: Path,
    overall_metrics: Dict[str, float],
    slice_metrics: Dict[str, List[SliceMetrics]],
    disparities: Dict[str, float],
) -> Path:
    """Save metrics to JSON for downstream tracking."""
    payload = {
        "overall": overall_metrics,
        "slices": {name: [asdict(row) for row in rows] for name, rows in slice_metrics.items()},
        "disparities": disparities,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / "bias_metrics.json"
    target.write_text(json.dumps(payload, indent=2))
    LOGGER.info("Wrote metrics to %s", target)
    return target


def resolve_path(uri_or_path: str, fetcher: Optional[GCSArtifactFetcher], destination: Path) -> Path:
    """Download from GCS when necessary, otherwise return the local path."""
    if is_gcs_uri(uri_or_path):
        if fetcher is None:
            fetcher = GCSArtifactFetcher()
        if uri_or_path.lower().endswith(".csv"):
            return fetcher.download_file(uri_or_path, destination)
        return fetcher.download_artifact(uri_or_path, destination)
    return Path(uri_or_path).expanduser().resolve()


def parse_arguments() -> argparse.Namespace:
    """CLI definition."""
    parser = argparse.ArgumentParser(description="Evaluate fine-tuned transformer fairness slices.")
    parser.add_argument(
        "--model-uri",
        default=None,
        help="Local path or gs:// URI for the HF model directory. "
        "Defaults to env-derived gs://MODEL_BUCKET/MODEL_NAME/vMODEL_VERSION/ when present.",
    )
    parser.add_argument(
        "--data-uri",
        default=None,
        help="Local path or gs:// URI for the labeled CSV file. "
        "Defaults to DATASET_URI env or gs://{DATA_BUCKET}/{DATA_FOLDER}/{DATA_FILE}.",
    )
    parser.add_argument(
        "--workdir",
        default="Bias_Detection/artifacts",
        help="Directory to store downloaded artifacts and evaluation outputs.",
    )
    parser.add_argument(
        "--model-cache-dir",
        default=None,
        help="Directory used to cache downloaded model weights (defaults to MODEL_TMP_PATH env or /tmp/<model>).",
    )
    parser.add_argument(
        "--slice-cols",
        nargs="+",
        default=list(DEFAULT_SLICE_COLS),
        help="Metadata columns used to compute per-slice metrics.",
    )
    parser.add_argument("--batch-size", type=int, default=16, help="Transformer inference batch size.")
    parser.add_argument("--max-length", type=int, default=512, help="Maximum token length during tokenization.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    parser.add_argument(
        "--gcp-credentials",
        default=None,
        help="Path to a service-account JSON. Defaults to GOOGLE_APPLICATION_CREDENTIALS, "
        "GCP_CREDENTIALS_PATH, or ./gcp.json when present.",
    )
    parser.add_argument(
        "--tokenizer-name",
        default=TOKENIZER_NAME,
        help="Tokenizer identifier to load (defaults to TOKENIZER_NAME env or distilbert-base-uncased).",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point for CLI usage."""
    args = parse_arguments()
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")

    workdir = Path(args.workdir).expanduser()
    workdir.mkdir(parents=True, exist_ok=True)

    model_uri = args.model_uri or build_model_uri_from_env()
    if not model_uri:
        raise ValueError(
            "Model location not provided. Either set MODEL_BUCKET/MODEL_NAME/MODEL_VERSION env "
            "variables or pass --model-uri explicitly."
        )

    data_uri = args.data_uri or build_data_uri_from_env()
    if not data_uri:
        raise ValueError("Dataset location not provided. Set DATASET_URI env or pass --data-uri.")

    model_cache_dir = Path(args.model_cache_dir).expanduser() if args.model_cache_dir else default_model_cache_dir()
    credentials_path = resolve_credentials_path(args.gcp_credentials)

    fetcher: Optional[GCSArtifactFetcher] = None
    if is_gcs_uri(model_uri) or is_gcs_uri(data_uri):
        fetcher = GCSArtifactFetcher(credentials_path=credentials_path)

    model_path = resolve_path(model_uri, fetcher, model_cache_dir)
    data_path = resolve_path(data_uri, fetcher, workdir / "dataset.csv")

    df = load_dataframe(data_path)
    evaluator = BiasEvaluator(
        model_dir=model_path,
        batch_size=args.batch_size,
        max_length=args.max_length,
        tokenizer_name=args.tokenizer_name,
    )
    predictions, confidences = evaluator.predict(df)
    df["prediction"] = predictions
    df["confidence"] = confidences

    overall_metrics = evaluator.compute_classification_metrics(df["label"], df["prediction"])
    slice_metrics = evaluator.evaluate_slices(df, df["label"], df["prediction"], args.slice_cols)
    disparities = summarize_disparities(slice_metrics)
    write_metrics(workdir, overall_metrics, slice_metrics, disparities)

    # LOGGER.info("Overall metrics: %s", overall_metrics)
    # LOGGER.info("Slice disparities (F1 range): %s", disparities or "None detected")

    # for slice_name, rows in slice_metrics.items():
    #     LOGGER.info("--- %s ---", slice_name)
    #     for row in rows:
    #         LOGGER.info(
    #             "%s=%s | count=%d | acc=%.3f | prec=%.3f | rec=%.3f | f1=%.3f",
    #             slice_name,
    #             row.slice_value,
    #             row.count,
    #             row.accuracy,
    #             row.precision,
    #             row.recall,
    #             row.f1,
    #         )

if __name__ == "__main__":
    main()
