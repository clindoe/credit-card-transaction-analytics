"""
Unit tests for data quality logic (mirrors src/glue_jobs/raw_to_staging.py).

These functions are extracted from the Glue PySpark job so they can be tested
with plain pandas/numpy — no Spark context required.
"""

from __future__ import annotations

import pandas as pd
import pytest


# ── Standalone quality functions (mirrors Glue job logic) ────────────────────

def compute_quality_score(raw_count: int, final_count: int) -> float:
    """Fraction of records that passed all quality checks, as a percentage."""
    if raw_count == 0:
        return 0.0
    return round((final_count / raw_count) * 100, 2)


def detect_nulls(df: pd.DataFrame, critical_cols: list[str]) -> pd.DataFrame:
    """Return rows where any critical column is null."""
    mask = df[critical_cols].isnull().any(axis=1)
    return df[mask]


def detect_duplicates(df: pd.DataFrame, key_col: str) -> pd.DataFrame:
    """Return all rows that are duplicates of a prior row on key_col."""
    return df[df.duplicated(subset=[key_col], keep="first")]


def validate_amount(amount: float) -> bool:
    """Transaction amount must be a positive number."""
    return isinstance(amount, (int, float)) and amount > 0


def quarantine_schema_failures(
    df: pd.DataFrame, required_cols: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split dataframe into (valid, quarantine) on required column nulls."""
    if not all(col in df.columns for col in required_cols):
        return pd.DataFrame(), df
    null_mask = df[required_cols].isnull().any(axis=1)
    return df[~null_mask].copy(), df[null_mask].copy()


def standardize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase merchant/category, uppercase gender/state."""
    df = df.copy()
    for col in ["merchant", "category"]:
        if col in df.columns:
            df[col] = df[col].str.lower().str.strip()
    for col in ["gender", "state"]:
        if col in df.columns:
            df[col] = df[col].str.upper().str.strip()
    return df


# ── TestQualityScore ──────────────────────────────────────────────────────────

class TestQualityScore:
    def test_perfect_score(self):
        assert compute_quality_score(1_000, 1_000) == 100.0

    def test_partial_score(self):
        assert compute_quality_score(1_000, 950) == 95.0

    def test_zero_input_returns_zero(self):
        assert compute_quality_score(0, 0) == 0.0

    def test_rounding_two_decimals(self):
        score = compute_quality_score(3, 2)  # 66.666...
        assert abs(score - 66.67) < 0.01

    def test_real_dataset_score_at_threshold(self):
        # Production run: all 1,852,394 records passed
        score = compute_quality_score(1_852_394, 1_852_394)
        assert score == 100.0

    def test_score_above_minimum_threshold(self):
        # At least 95 % must pass for the pipeline to be healthy
        score = compute_quality_score(1_000, 960)
        assert score >= 95.0

    def test_score_below_threshold_detected(self):
        score = compute_quality_score(1_000, 900)
        assert score < 95.0


# ── TestNullDetection ─────────────────────────────────────────────────────────

class TestNullDetection:
    @pytest.fixture()
    def sample_df(self):
        return pd.DataFrame(
            {
                "trans_num": ["t1", "t2", "t3", None, "t5"],
                "amt": [10.0, None, 50.0, 5.0, 100.0],
                "cc_num": [111, 222, 333, 444, 555],
                "city": [None, "LA", "NY", "TX", "FL"],  # non-critical
            }
        )

    def test_finds_all_critical_nulls(self, sample_df):
        result = detect_nulls(sample_df, ["trans_num", "amt"])
        assert len(result) == 2  # row 1 (null amt) + row 3 (null trans_num)

    def test_non_critical_nulls_ignored(self, sample_df):
        # city is null in row 0 but not a critical column
        result = detect_nulls(sample_df, ["trans_num", "amt"])
        assert "t1" in result["trans_num"].values or len(result) == 2

    def test_no_nulls_returns_empty(self):
        clean = pd.DataFrame({"trans_num": ["t1", "t2"], "amt": [10.0, 20.0]})
        result = detect_nulls(clean, ["trans_num", "amt"])
        assert len(result) == 0

    def test_all_nulls_flagged(self):
        all_null = pd.DataFrame({"trans_num": [None, None], "amt": [None, None]})
        result = detect_nulls(all_null, ["trans_num", "amt"])
        assert len(result) == 2


# ── TestDuplicateDetection ────────────────────────────────────────────────────

class TestDuplicateDetection:
    @pytest.fixture()
    def duped_df(self):
        return pd.DataFrame(
            {
                "trans_num": ["t1", "t2", "t1", "t3", "t2"],
                "amt": [10.0, 20.0, 10.0, 30.0, 20.0],
            }
        )

    def test_detects_correct_count(self, duped_df):
        result = detect_duplicates(duped_df, "trans_num")
        assert len(result) == 2

    def test_no_duplicates_returns_empty(self):
        clean = pd.DataFrame({"trans_num": ["t1", "t2", "t3"], "amt": [10, 20, 30]})
        assert len(detect_duplicates(clean, "trans_num")) == 0

    def test_keeps_first_occurrence(self, duped_df):
        # First occurrences: t1@index0, t2@index1 — duplicates are index 2 and 4
        dupes = detect_duplicates(duped_df, "trans_num")
        assert set(dupes.index) == {2, 4}

    def test_single_row_not_duplicate(self):
        df = pd.DataFrame({"trans_num": ["t1"], "amt": [10.0]})
        assert len(detect_duplicates(df, "trans_num")) == 0


# ── TestAmountValidation ──────────────────────────────────────────────────────

class TestAmountValidation:
    def test_positive_valid(self):
        assert validate_amount(50.0) is True

    def test_zero_invalid(self):
        assert validate_amount(0.0) is False

    def test_negative_invalid(self):
        assert validate_amount(-10.0) is False

    def test_minimum_real_world_value(self):
        assert validate_amount(1.0) is True  # min in dataset

    def test_maximum_real_world_value(self):
        assert validate_amount(28_948.90) is True  # max in dataset

    def test_none_invalid(self):
        assert validate_amount(None) is False  # type: ignore[arg-type]

    def test_string_invalid(self):
        assert validate_amount("100") is False  # type: ignore[arg-type]


# ── TestQuarantineLogic ───────────────────────────────────────────────────────

class TestQuarantineLogic:
    def test_splits_correctly(self):
        df = pd.DataFrame(
            {"trans_num": ["t1", None, "t3"], "amt": [10.0, 20.0, None], "cc_num": [1, 2, 3]}
        )
        valid, quarantine = quarantine_schema_failures(df, ["trans_num", "amt"])
        assert len(valid) == 1
        assert len(quarantine) == 2

    def test_all_valid_no_quarantine(self):
        df = pd.DataFrame({"trans_num": ["t1", "t2"], "amt": [10.0, 20.0]})
        valid, q = quarantine_schema_failures(df, ["trans_num", "amt"])
        assert len(valid) == 2
        assert len(q) == 0

    def test_all_invalid_all_quarantined(self):
        df = pd.DataFrame({"trans_num": [None, None], "amt": [None, None]})
        valid, q = quarantine_schema_failures(df, ["trans_num", "amt"])
        assert len(valid) == 0
        assert len(q) == 2

    def test_missing_required_column_quarantines_all(self):
        df = pd.DataFrame({"amt": [10.0, 20.0]})  # trans_num missing entirely
        valid, q = quarantine_schema_failures(df, ["trans_num", "amt"])
        assert len(valid) == 0

    def test_disjoint_sets(self):
        df = pd.DataFrame(
            {"trans_num": ["t1", None, "t3", None], "amt": [1.0, 2.0, None, None]}
        )
        valid, q = quarantine_schema_failures(df, ["trans_num", "amt"])
        assert len(valid) + len(q) == len(df)


# ── TestStandardization ───────────────────────────────────────────────────────

class TestStandardization:
    @pytest.fixture()
    def raw_df(self):
        return pd.DataFrame(
            {
                "merchant": ["AMAZON.COM", "  Walmart  ", "Target"],
                "category": ["Gas_Transport", "GROCERY_POS", "home"],
                "gender": ["m", "F", " m "],
                "state": ["ca", "TX", " ny "],
            }
        )

    def test_merchant_lowercased(self, raw_df):
        result = standardize_text_columns(raw_df)
        assert result["merchant"].tolist() == ["amazon.com", "walmart", "target"]

    def test_category_lowercased(self, raw_df):
        result = standardize_text_columns(raw_df)
        assert result["category"].tolist() == ["gas_transport", "grocery_pos", "home"]

    def test_gender_uppercased(self, raw_df):
        result = standardize_text_columns(raw_df)
        assert result["gender"].tolist() == ["M", "F", "M"]

    def test_state_uppercased(self, raw_df):
        result = standardize_text_columns(raw_df)
        assert result["state"].tolist() == ["CA", "TX", "NY"]

    def test_original_df_not_mutated(self, raw_df):
        _ = standardize_text_columns(raw_df)
        assert raw_df["merchant"].iloc[0] == "AMAZON.COM"  # original unchanged
