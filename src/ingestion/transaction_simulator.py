"""
transaction_simulator.py — Reads Kaggle CSV and ingests into S3 in batches.

Simulates real-time data arrival by:
1. Reading the CSV in configurable batch sizes (default 1000 rows)
2. Adding ingestion metadata (timestamp, batch ID, source system)
3. Converting each batch to Parquet (columnar, compressed, schema-embedded)
4. Uploading to S3 with Hive-style partitioning (year/month/day)
5. Logging every batch to a manifest file for lineage tracking
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import os
import sys
import tempfile
import logging
from datetime import datetime, timezone

# Import our reusable S3 uploader
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from src.ingestion.s3_uploader import upload_file, upload_bytes, build_s3_key, BUCKET_NAME

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
BATCH_SIZE = 1000               # Rows per batch (Parquet file)
SOURCE_SYSTEM = "kaggle_fraud_detection_v1"
TEMP_DIR = tempfile.gettempdir()
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def validate_batch(df: pd.DataFrame, batch_id: int) -> pd.DataFrame:
    """
    Basic data validation before upload.
    - Drops rows with null transaction amounts or dates
    - Drops rows with null transaction IDs
    - Logs how many rows were dropped

    Args:
        df: Batch DataFrame
        batch_id: Current batch number (for logging)

    Returns:
        Cleaned DataFrame
    """
    original_count = len(df)

    # Check for critical nulls
    df = df.dropna(subset=["trans_date_trans_time", "amt", "trans_num"])

    dropped = original_count - len(df)
    if dropped > 0:
        logger.warning(f"Batch {batch_id}: Dropped {dropped} rows with null critical fields")

    # Check for negative amounts (data quality flag)
    negative_count = (df["amt"] < 0).sum()
    if negative_count > 0:
        logger.warning(f"Batch {batch_id}: Found {negative_count} negative amounts")

    return df


def add_metadata(df: pd.DataFrame, batch_id: int) -> pd.DataFrame:
    """
    Add ingestion metadata columns to the batch.

    Args:
        df: Batch DataFrame
        batch_id: Current batch number

    Returns:
        DataFrame with metadata columns added
    """
    df = df.copy()
    df["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    df["batch_id"] = batch_id
    df["source_system"] = SOURCE_SYSTEM
    return df


def extract_partition_date(df: pd.DataFrame) -> tuple:
    """
    Extract the most common (year, month, day) from the batch
    to determine the Hive partition.

    The dataset has a 'trans_date_trans_time' column with format:
    '2019-01-01 00:00:18'

    Args:
        df: Batch DataFrame

    Returns:
        (year, month, day) tuple
    """
    dates = pd.to_datetime(df["trans_date_trans_time"])
    # Use the date that appears most often in this batch
    most_common_date = dates.dt.date.mode()[0]
    return most_common_date.year, most_common_date.month, most_common_date.day


def batch_to_parquet(df: pd.DataFrame, batch_id: int) -> str:
    """
    Convert a batch DataFrame to a local Parquet file.

    Args:
        df: Batch DataFrame
        batch_id: Current batch number

    Returns:
        Path to the local Parquet file
    """
    filename = f"batch_{batch_id:06d}.parquet"
    local_path = os.path.join(TEMP_DIR, filename)

    table = pa.Table.from_pandas(df)
    pq.write_table(table, local_path, compression="snappy")

    file_size = os.path.getsize(local_path) / 1024
    logger.info(f"Batch {batch_id}: Wrote {len(df)} rows to Parquet ({file_size:.1f} KB)")

    return local_path


def create_manifest_entry(batch_id: int, row_count: int, s3_key: str,
                          partition_date: tuple, start_time: datetime) -> dict:
    """
    Create a manifest entry for lineage tracking.

    Args:
        batch_id: Current batch number
        row_count: Number of rows in the batch
        s3_key: S3 key where the batch was uploaded
        partition_date: (year, month, day) tuple
        start_time: When processing of this batch started

    Returns:
        Manifest entry dictionary
    """
    return {
        "batch_id": batch_id,
        "row_count": row_count,
        "s3_key": s3_key,
        "s3_bucket": BUCKET_NAME,
        "partition_year": partition_date[0],
        "partition_month": partition_date[1],
        "partition_day": partition_date[2],
        "source_system": SOURCE_SYSTEM,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "processing_time_seconds": (datetime.now(timezone.utc) - start_time).total_seconds(),
        "status": "success"
    }


def ingest_csv(csv_path: str, source_label: str = "train") -> dict:
    """
    Main ingestion function. Reads a CSV in batches and uploads each
    batch to S3 as a Parquet file with Hive-style partitioning.

    Args:
        csv_path: Path to the CSV file
        source_label: Label for this source file (used in logging)

    Returns:
        Summary dict with total stats
    """
    logger.info(f"═══ Starting ingestion: {csv_path} ═══")
    logger.info(f"Bucket: {BUCKET_NAME} | Batch size: {BATCH_SIZE}")

    manifest_entries = []
    total_rows = 0
    total_batches = 0
    failed_batches = 0

    ingestion_run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Read CSV in chunks to keep memory usage low
    reader = pd.read_csv(csv_path, chunksize=BATCH_SIZE)

    for batch_id, chunk in enumerate(reader, start=1):
        batch_start = datetime.now(timezone.utc)

        try:
            # Step 1: Validate
            clean_chunk = validate_batch(chunk, batch_id)
            if len(clean_chunk) == 0:
                logger.warning(f"Batch {batch_id}: All rows dropped during validation, skipping")
                continue

            # Step 2: Add metadata
            enriched_chunk = add_metadata(clean_chunk, batch_id)

            # Step 3: Determine partition
            year, month, day = extract_partition_date(enriched_chunk)

            # Step 4: Convert to Parquet
            local_parquet = batch_to_parquet(enriched_chunk, batch_id)

            # Step 5: Build S3 key and upload
            filename = f"batch_{batch_id:06d}.parquet"
            s3_key = build_s3_key("raw", "transactions", year, month, day, filename)
            success = upload_file(local_parquet, s3_key)

            if success:
                total_rows += len(enriched_chunk)
                total_batches += 1

                # Step 6: Log to manifest
                entry = create_manifest_entry(
                    batch_id, len(enriched_chunk), s3_key,
                    (year, month, day), batch_start
                )
                manifest_entries.append(entry)
            else:
                failed_batches += 1
                logger.error(f"Batch {batch_id}: Upload failed")

            # Step 7: Clean up temp file
            os.remove(local_parquet)

            # Progress update every 100 batches
            if batch_id % 100 == 0:
                logger.info(
                    f"──── Progress: {batch_id} batches processed, "
                    f"{total_rows:,} rows ingested ────"
                )

        except Exception as e:
            failed_batches += 1
            logger.error(f"Batch {batch_id}: Unexpected error — {e}")
            continue

    # Upload the manifest file
    manifest = {
        "ingestion_run_id": ingestion_run_id,
        "source_file": os.path.basename(csv_path),
        "source_label": source_label,
        "total_batches": total_batches,
        "total_rows": total_rows,
        "failed_batches": failed_batches,
        "batch_size": BATCH_SIZE,
        "entries": manifest_entries
    }

    manifest_key = f"metadata/manifests/{source_label}_manifest_{ingestion_run_id}.json"
    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
    upload_bytes(manifest_bytes, manifest_key)

    logger.info("═══════════════════════════════════════")
    logger.info(f"INGESTION COMPLETE: {source_label}")
    logger.info(f"  Total rows ingested: {total_rows:,}")
    logger.info(f"  Total batches: {total_batches}")
    logger.info(f"  Failed batches: {failed_batches}")
    logger.info(f"  Manifest: s3://{BUCKET_NAME}/{manifest_key}")
    logger.info("═══════════════════════════════════════")

    return manifest


# ──────────────────────────────────────────────
# MAIN — Run from command line
# ──────────────────────────────────────────────
if __name__ == "__main__":
    import time

    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    train_csv = os.path.join(project_root, "data", "fraudTrain.csv")
    test_csv = os.path.join(project_root, "data", "fraudTest.csv")

    overall_start = time.time()

    # Ingest training data
    if os.path.exists(train_csv):
        train_summary = ingest_csv(train_csv, source_label="train")
    else:
        logger.error(f"Training CSV not found at {train_csv}")
        sys.exit(1)

    # Ingest test data
    if os.path.exists(test_csv):
        test_summary = ingest_csv(test_csv, source_label="test")
    else:
        logger.warning(f"Test CSV not found at {test_csv} — skipping")

    elapsed = time.time() - overall_start
    logger.info(f"Total ingestion time: {elapsed / 60:.1f} minutes")