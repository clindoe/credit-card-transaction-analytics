"""
s3_uploader.py — Reusable S3 upload utility for the credit card analytics pipeline.

Handles:
- Uploading files (Parquet, JSON, CSV) to S3
- Hive-style partition path construction
- Upload verification
- Error handling with retries
"""

import boto3
import os
import logging
from botocore.exceptions import ClientError

# ──────────────────────────────────────────────
# CONFIGURATION — UPDATE THIS WITH YOUR BUCKET NAME
# ──────────────────────────────────────────────
BUCKET_NAME = "credit-txn-analytics-adil"   # 
REGION = "us-east-1"
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Create a single S3 client to reuse across uploads (avoids re-creating connections)
s3_client = boto3.client("s3", region_name=REGION)


def build_s3_key(zone: str, table: str, year: int, month: int, day: int, filename: str) -> str:
    """
    Build a Hive-style partitioned S3 key.

    Example output:
        raw/transactions/year=2019/month=01/day=15/batch_001.parquet

    Args:
        zone: S3 prefix zone (raw, staging, analytics, exports)
        table: Table or dataset name (e.g., 'transactions')
        year: Partition year
        month: Partition month
        day: Partition day
        filename: Name of the file being uploaded

    Returns:
        Full S3 key string
    """
    return f"{zone}/{table}/year={year}/month={month:02d}/day={day:02d}/{filename}"


def upload_file(local_path: str, s3_key: str, max_retries: int = 3) -> bool:
    """
    Upload a local file to S3 with retry logic.

    Args:
        local_path: Path to the file on your machine
        s3_key: Destination key in S3
        max_retries: Number of retry attempts on failure

    Returns:
        True if upload succeeded, False otherwise
    """
    for attempt in range(1, max_retries + 1):
        try:
            file_size = os.path.getsize(local_path)
            logger.info(
                f"Uploading {local_path} → s3://{BUCKET_NAME}/{s3_key} "
                f"({file_size / 1024:.1f} KB) [attempt {attempt}/{max_retries}]"
            )

            s3_client.upload_file(local_path, BUCKET_NAME, s3_key)

            # Verify the upload by checking the object exists and size matches
            response = s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
            remote_size = response["ContentLength"]

            if remote_size == file_size:
                logger.info(f"Upload verified: {s3_key} ({remote_size} bytes)")
                return True
            else:
                logger.warning(
                    f"Size mismatch: local={file_size}, remote={remote_size}. Retrying..."
                )

        except ClientError as e:
            logger.error(f"S3 error on attempt {attempt}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt}: {e}")

    logger.error(f"FAILED to upload {local_path} after {max_retries} attempts")
    return False


def upload_bytes(data: bytes, s3_key: str) -> bool:
    """
    Upload raw bytes (e.g., JSON string) directly to S3.

    Args:
        data: Bytes to upload
        s3_key: Destination key in S3

    Returns:
        True if upload succeeded, False otherwise
    """
    try:
        s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=data)
        logger.info(f"Uploaded bytes → s3://{BUCKET_NAME}/{s3_key} ({len(data)} bytes)")
        return True
    except ClientError as e:
        logger.error(f"Failed to upload bytes to {s3_key}: {e}")
        return False


def list_objects(prefix: str) -> list:
    """
    List all objects under a given S3 prefix.

    Args:
        prefix: S3 prefix to list (e.g., 'raw/transactions/')

    Returns:
        List of S3 keys
    """
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return keys