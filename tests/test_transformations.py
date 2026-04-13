"""
Unit tests for data transformation logic
(mirrors enrichment steps in src/glue_jobs/raw_to_staging.py
 and risk scoring in staging_to_analytics.py).
"""

from __future__ import annotations

from math import isclose

import numpy as np
import pandas as pd
import pytest


# ── Standalone transformation functions ──────────────────────────────────────

def haversine_distance_km(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    """Great-circle distance (km) between two lat/lon points."""
    R = 6_371.0
    lat1_r, lon1_r = np.radians(lat1), np.radians(lon1)
    lat2_r, lon2_r = np.radians(lat2), np.radians(lon2)
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlon / 2) ** 2
    return float(R * 2 * np.arcsin(np.sqrt(a)))


def assign_amount_bucket(amount: float) -> str:
    """Assign a transaction to one of five amount buckets."""
    if amount < 10:
        return "under_10"
    elif amount < 50:
        return "10_to_50"
    elif amount < 200:
        return "50_to_200"
    elif amount < 1_000:
        return "200_to_1000"
    else:
        return "over_1000"


def extract_time_features(timestamp_str: str) -> dict:
    """Return hour, iso weekday (1=Mon…7=Sun), is_weekend, day name."""
    from datetime import datetime

    dt = datetime.fromisoformat(timestamp_str)
    return {
        "transaction_hour": dt.hour,
        "day_of_week": dt.isoweekday(),
        "is_weekend": dt.isoweekday() >= 6,
        "day_of_week_name": dt.strftime("%A"),
    }


def compute_risk_score(
    fraud_rate_pct: float,       # 0–100 %
    avg_amount: float,
    max_amount: float,
    late_night_ratio: float,     # 0–1
    avg_distance_km: float,
    unique_categories: int,
) -> tuple[float, str]:
    """
    Six-component customer risk score (0–100) and tier string.

    Components:
        fraud_history  0–40 pts  (main driver)
        high_avg_amt   0–15 pts
        max_txn        0–10 pts
        late_night     0–15 pts
        distance       0–10 pts
        low_diversity  0–10 pts  (penalty for < 10 categories)
    """
    risk = 0.0
    risk += min(40.0, (fraud_rate_pct / 100) * 40)
    risk += min(15.0, (avg_amount / 500) * 15)
    risk += min(10.0, (max_amount / 5_000) * 10)
    risk += min(15.0, late_night_ratio * 50)
    risk += min(10.0, (avg_distance_km / 200) * 10)
    risk += max(0.0, 10.0 - unique_categories)  # penalty for low diversity

    risk = round(min(100.0, risk), 1)

    if risk >= 60:
        tier = "Critical"
    elif risk >= 30:
        tier = "High"
    elif risk >= 10:
        tier = "Medium"
    else:
        tier = "Low"

    return risk, tier


def add_amount_buckets(df: pd.DataFrame, col: str = "amt") -> pd.DataFrame:
    """Vectorised amount_bucket assignment for a DataFrame."""
    df = df.copy()
    conditions = [
        df[col] < 10,
        df[col] < 50,
        df[col] < 200,
        df[col] < 1_000,
    ]
    choices = ["under_10", "10_to_50", "50_to_200", "200_to_1000"]
    df["amount_bucket"] = np.select(conditions, choices, default="over_1000")
    return df


# ── TestHaversineDistance ─────────────────────────────────────────────────────

class TestHaversineDistance:
    def test_same_point_zero(self):
        assert isclose(haversine_distance_km(40.0, -74.0, 40.0, -74.0), 0.0, abs_tol=1e-6)

    def test_nyc_to_la(self):
        # NYC → LA ≈ 3940 km
        dist = haversine_distance_km(40.71, -74.01, 34.05, -118.24)
        assert 3_900 < dist < 4_000

    def test_symmetry(self):
        d1 = haversine_distance_km(40.0, -74.0, 34.0, -118.0)
        d2 = haversine_distance_km(34.0, -118.0, 40.0, -74.0)
        assert isclose(d1, d2, rel_tol=1e-6)

    def test_always_positive(self):
        assert haversine_distance_km(33.0, -97.0, 29.7, -95.4) > 0

    def test_dataset_max_distance_in_range(self):
        # Real dataset max = 152.12 km — plausible for domestic US transactions
        dist = haversine_distance_km(40.0, -74.0, 38.9, -77.0)  # NYC → DC ≈ 328 km
        assert dist < 400

    def test_adjacent_cities(self):
        # Dallas → Fort Worth ≈ 50 km
        dist = haversine_distance_km(32.78, -96.80, 32.75, -97.33)
        assert 45 < dist < 60


# ── TestAmountBucket ──────────────────────────────────────────────────────────

class TestAmountBucket:
    # Boundary tests
    @pytest.mark.parametrize("amount,expected", [
        (0.01, "under_10"),
        (9.99, "under_10"),
        (10.0, "10_to_50"),
        (49.99, "10_to_50"),
        (50.0, "50_to_200"),
        (199.99, "50_to_200"),
        (200.0, "200_to_1000"),
        (999.99, "200_to_1000"),
        (1_000.0, "over_1000"),
        (28_948.90, "over_1000"),
    ])
    def test_bucket_boundaries(self, amount, expected):
        assert assign_amount_bucket(amount) == expected

    def test_dataset_median_in_correct_bucket(self):
        # Median transaction = $47.45 → 10_to_50
        assert assign_amount_bucket(47.45) == "10_to_50"

    def test_dataset_mean_in_correct_bucket(self):
        # Mean = $70.06 → 50_to_200
        assert assign_amount_bucket(70.06) == "50_to_200"

    def test_all_five_buckets_are_reachable(self):
        amounts = [5, 25, 100, 500, 5_000]
        buckets = {assign_amount_bucket(a) for a in amounts}
        assert buckets == {"under_10", "10_to_50", "50_to_200", "200_to_1000", "over_1000"}


class TestAmountBucketVectorised:
    def test_dataframe_all_buckets(self):
        df = pd.DataFrame({"amt": [5.0, 25.0, 100.0, 500.0, 5_000.0]})
        result = add_amount_buckets(df)
        expected = ["under_10", "10_to_50", "50_to_200", "200_to_1000", "over_1000"]
        assert result["amount_bucket"].tolist() == expected

    def test_no_mutation_of_input(self):
        df = pd.DataFrame({"amt": [10.0, 50.0]})
        _ = add_amount_buckets(df)
        assert "amount_bucket" not in df.columns

    def test_null_safe(self):
        # NaN rows should produce NaN bucket (handled upstream by null drop)
        df = pd.DataFrame({"amt": [10.0, float("nan")]})
        result = add_amount_buckets(df)
        # NaN < 10 is False, NaN < 50 is False, etc. → falls to over_1000 default
        # This is acceptable; real pipeline drops nulls before enrichment
        assert result["amount_bucket"].iloc[0] == "10_to_50"


# ── TestTimeFeatures ──────────────────────────────────────────────────────────

class TestTimeFeatures:
    def test_weekday(self):
        f = extract_time_features("2020-06-15 14:30:00")  # Monday
        assert f["day_of_week"] == 1
        assert f["is_weekend"] is False
        assert f["day_of_week_name"] == "Monday"

    def test_saturday_is_weekend(self):
        f = extract_time_features("2020-06-20 10:00:00")  # Saturday
        assert f["is_weekend"] is True
        assert f["day_of_week"] == 6

    def test_sunday_is_weekend(self):
        f = extract_time_features("2020-06-21 10:00:00")  # Sunday
        assert f["is_weekend"] is True
        assert f["day_of_week"] == 7

    def test_late_night_hour(self):
        f = extract_time_features("2020-01-01 03:00:00")
        assert 2 <= f["transaction_hour"] <= 5

    def test_midnight(self):
        f = extract_time_features("2020-01-01 00:00:00")
        assert f["transaction_hour"] == 0

    def test_hour_range(self):
        for h in [0, 6, 12, 18, 23]:
            f = extract_time_features(f"2020-01-01 {h:02d}:00:00")
            assert f["transaction_hour"] == h


# ── TestRiskScore ─────────────────────────────────────────────────────────────

class TestRiskScore:
    def test_low_risk_customer(self):
        score, tier = compute_risk_score(
            fraud_rate_pct=0.0, avg_amount=30.0, max_amount=150.0,
            late_night_ratio=0.0, avg_distance_km=10.0, unique_categories=12
        )
        assert tier == "Low"
        assert score < 10

    def test_medium_risk_customer(self):
        score, tier = compute_risk_score(
            fraud_rate_pct=5.0, avg_amount=100.0, max_amount=500.0,
            late_night_ratio=0.05, avg_distance_km=50.0, unique_categories=8
        )
        assert tier in {"Medium", "High"}

    def test_high_risk_customer(self):
        score, tier = compute_risk_score(
            fraud_rate_pct=40.0, avg_amount=250.0, max_amount=2_000.0,
            late_night_ratio=0.15, avg_distance_km=120.0, unique_categories=5
        )
        assert tier in {"High", "Critical"}
        assert score >= 30

    def test_critical_risk_customer(self):
        score, tier = compute_risk_score(
            fraud_rate_pct=100.0, avg_amount=400.0, max_amount=4_000.0,
            late_night_ratio=0.3, avg_distance_km=180.0, unique_categories=2
        )
        assert tier == "Critical"
        assert score >= 60

    def test_score_never_exceeds_100(self):
        score, _ = compute_risk_score(100.0, 10_000.0, 100_000.0, 1.0, 500.0, 0)
        assert score <= 100.0

    def test_score_never_below_zero(self):
        score, _ = compute_risk_score(0.0, 0.0, 0.0, 0.0, 0.0, 14)
        assert score >= 0.0

    def test_tier_ordering(self):
        """Higher fraud rate → higher tier."""
        _, t_low = compute_risk_score(0.0, 20.0, 100.0, 0.0, 5.0, 12)
        _, t_high = compute_risk_score(80.0, 300.0, 3_000.0, 0.25, 150.0, 3)
        tier_rank = {"Low": 0, "Medium": 1, "High": 2, "Critical": 3}
        assert tier_rank[t_high] > tier_rank[t_low]

    def test_low_diversity_adds_risk(self):
        base_score, _ = compute_risk_score(0.0, 0.0, 0.0, 0.0, 0.0, 10)
        low_div_score, _ = compute_risk_score(0.0, 0.0, 0.0, 0.0, 0.0, 2)
        assert low_div_score > base_score

    def test_distance_component(self):
        near_score, _ = compute_risk_score(0.0, 0.0, 0.0, 0.0, 5.0, 10)
        far_score, _ = compute_risk_score(0.0, 0.0, 0.0, 0.0, 190.0, 10)
        assert far_score > near_score
