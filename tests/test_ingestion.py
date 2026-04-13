"""
test_ingestion.py — Unit tests for the ingestion scripts.

Run with: python -m pytest tests/ -v
"""

import pytest
import pandas as pd
import os
import pyarrow.parquet as pq

# We test functions in isolation — no actual S3 uploads in unit tests
from src.ingestion.transaction_simulator import (
    validate_batch,
    add_metadata,
    extract_partition_date,
    batch_to_parquet,
)


# ──────────────────────────────────────────────
# FIXTURES — Reusable test data
# ──────────────────────────────────────────────
@pytest.fixture
def sample_batch():
    """A small valid batch that mimics the real dataset."""
    return pd.DataFrame({
        "Unnamed: 0": [0, 1, 2, 3, 4],
        "trans_date_trans_time": [
            "2019-01-01 00:00:18",
            "2019-01-01 00:00:44",
            "2019-01-01 00:00:51",
            "2019-01-01 00:01:16",
            "2019-01-01 00:03:06",
        ],
        "cc_num": [123456789, 987654321, 111222333, 444555666, 777888999],
        "merchant": ["fraud_Rippin", "fraud_Heller", "fraud_Lind", "fraud_Kutch", "fraud_Keeling"],
        "category": ["misc_net", "grocery_pos", "entertainment", "gas_transport", "misc_net"],
        "amt": [4.97, 107.23, 220.11, 45.00, 0.89],
        "first": ["Jeff", "Joanne", "Valerie", "Robert", "Susan"],
        "last": ["Elliott", "Williams", "Torres", "Smith", "Jones"],
        "gender": ["M", "F", "F", "M", "F"],
        "street": ["123 Main", "456 Oak", "789 Pine", "101 Elm", "202 Maple"],
        "city": ["Anytown", "Othertown", "Sometown", "Newtown", "Oldtown"],
        "state": ["NJ", "NY", "CA", "TX", "FL"],
        "zip": ["08901", "10001", "90210", "73301", "33101"],
        "lat": [40.49, 40.71, 34.05, 30.27, 25.76],
        "long": [-74.26, -74.01, -118.24, -97.74, -80.19],
        "city_pop": [50000, 8000000, 3900000, 950000, 450000],
        "job": ["engineer", "nurse", "teacher", "developer", "doctor"],
        "dob": ["1968-03-19", "1990-07-22", "1985-01-14", "1975-11-30", "2000-05-05"],
        "trans_num": ["txn001", "txn002", "txn003", "txn004", "txn005"],
        "unix_time": [1325376018, 1325376044, 1325376051, 1325376076, 1325376186],
        "merch_lat": [40.50, 40.72, 34.06, 30.28, 25.77],
        "merch_long": [-74.27, -74.02, -118.25, -97.75, -80.20],
        "is_fraud": [0, 0, 1, 0, 0],
    })


@pytest.fixture
def batch_with_nulls(sample_batch):
    """Batch with some null critical fields that should be dropped."""
    df = sample_batch.copy()
    df.loc[1, "amt"] = None                      # null amount
    df.loc[3, "trans_date_trans_time"] = None     # null datetime
    df.loc[4, "trans_num"] = None                 # null transaction ID
    return df


# ──────────────────────────────────────────────
# TESTS — validate_batch
# ──────────────────────────────────────────────
class TestValidateBatch:
    def test_clean_data_passes_through(self, sample_batch):
        result = validate_batch(sample_batch, batch_id=1)
        assert len(result) == 5, "All 5 rows should survive validation"

    def test_null_critical_fields_dropped(self, batch_with_nulls):
        result = validate_batch(batch_with_nulls, batch_id=1)
        assert len(result) == 2, "3 rows with nulls in critical fields should be dropped"

    def test_empty_batch_returns_empty(self):
        empty_df = pd.DataFrame({
            "trans_date_trans_time": [None],
            "amt": [None],
            "trans_num": [None],
        })
        result = validate_batch(empty_df, batch_id=1)
        assert len(result) == 0


# ──────────────────────────────────────────────
# TESTS — add_metadata
# ──────────────────────────────────────────────
class TestAddMetadata:
    def test_metadata_columns_added(self, sample_batch):
        result = add_metadata(sample_batch, batch_id=42)
        assert "ingestion_timestamp" in result.columns
        assert "batch_id" in result.columns
        assert "source_system" in result.columns

    def test_batch_id_correct(self, sample_batch):
        result = add_metadata(sample_batch, batch_id=42)
        assert (result["batch_id"] == 42).all()

    def test_original_data_unchanged(self, sample_batch):
        result = add_metadata(sample_batch, batch_id=1)
        # Original columns still present
        assert "amt" in result.columns
        assert "trans_num" in result.columns
        # Original row count unchanged
        assert len(result) == len(sample_batch)


# ──────────────────────────────────────────────
# TESTS — extract_partition_date
# ──────────────────────────────────────────────
class TestExtractPartitionDate:
    def test_correct_date_extracted(self, sample_batch):
        year, month, day = extract_partition_date(sample_batch)
        assert year == 2019
        assert month == 1
        assert day == 1

    def test_mixed_dates_uses_mode(self):
        df = pd.DataFrame({
            "trans_date_trans_time": [
                "2019-03-15 10:00:00",
                "2019-03-15 11:00:00",
                "2019-03-15 12:00:00",
                "2019-03-16 01:00:00",  # different day — minority
            ]
        })
        year, month, day = extract_partition_date(df)
        assert (year, month, day) == (2019, 3, 15), "Should use the most common date"


# ──────────────────────────────────────────────
# TESTS — batch_to_parquet
# ──────────────────────────────────────────────
class TestBatchToParquet:
    def test_parquet_file_created(self, sample_batch):
        enriched = add_metadata(sample_batch, batch_id=1)
        path = batch_to_parquet(enriched, batch_id=1)
        assert os.path.exists(path)
        assert path.endswith(".parquet")
        # Clean up
        os.remove(path)

    def test_parquet_readable_and_correct(self, sample_batch):
        enriched = add_metadata(sample_batch, batch_id=1)
        path = batch_to_parquet(enriched, batch_id=1)
        table = pq.read_table(path)
        assert table.num_rows == 5
        assert "ingestion_timestamp" in table.column_names
        assert "amt" in table.column_names
        os.remove(path)
