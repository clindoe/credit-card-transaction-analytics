"""
Data loading utilities for the Streamlit dashboard.

Priority: local JSON reports / CSV files → synthetic fallback data.
All functions are cached with st.cache_data (5-min TTL).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

# ── Project root detection ────────────────────────────────────────────────────


def _project_root() -> Path:
    """Walk up from this file until we find requirements.txt."""
    current = Path(__file__).resolve().parent
    for _ in range(6):
        if (current / "requirements.txt").exists():
            return current
        current = current.parent
    return Path(os.getcwd())


ROOT = _project_root()


# ── Quality reports ───────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_quality_reports() -> list[dict]:
    report_dir = ROOT / "temp_reports"
    reports = []
    if report_dir.exists():
        for path in sorted(report_dir.glob("quality_report_*.json")):
            with open(path) as f:
                reports.append(json.load(f))
    return reports if reports else [_synthetic_quality_report()]


def _synthetic_quality_report() -> dict:
    return {
        "report_timestamp": "2026-04-06T01:42:16.883866+00:00",
        "job_name": "raw-to-staging",
        "record_counts": {
            "raw_input": 1_852_394,
            "schema_failures": 0,
            "null_dropped": 0,
            "duplicates_removed": 0,
            "final_output": 1_852_394,
            "quality_score_pct": 100.0,
        },
        "null_counts_per_column": {},
        "amount_statistics": {
            "min": 1.0,
            "max": 28_948.9,
            "mean": 70.06,
            "stddev": 159.25,
            "median": 47.45,
        },
        "fraud_distribution": {"0": 1_842_743, "1": 9_651},
        "category_distribution": {
            "gas_transport": 188_029,
            "grocery_pos": 176_191,
            "home": 175_460,
            "shopping_pos": 166_463,
            "kids_pets": 161_727,
            "shopping_net": 139_322,
            "entertainment": 134_118,
            "food_dining": 130_729,
            "personal_care": 130_085,
            "health_fitness": 122_553,
            "misc_pos": 114_229,
            "misc_net": 90_654,
            "grocery_net": 64_878,
            "travel": 57_956,
        },
        "amount_bucket_distribution": {
            "under_10": 479_859,
            "10_to_50": 481_238,
            "50_to_200": 804_014,
            "200_to_1000": 81_763,
            "over_1000": 5_520,
        },
        "distance_statistics": {"min_km": 0.02, "max_km": 152.12, "mean_km": 76.11},
    }


# ── Ingestion manifests ───────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_manifests() -> list[dict]:
    manifest_dir = ROOT / "temp_manifests"
    manifests = []
    if manifest_dir.exists():
        for path in sorted(manifest_dir.glob("*_manifest_*.json")):
            with open(path) as f:
                raw = json.load(f)
            manifests.append(
                {
                    "Run ID": raw.get("ingestion_run_id", "unknown"),
                    "Source File": raw.get("source_file", "unknown"),
                    "Total Rows": raw.get("total_rows", 0),
                    "Total Batches": raw.get("total_batches", 0),
                    "Failed Batches": raw.get("failed_batches", 0),
                    "Batch Size": raw.get("batch_size", 1_000),
                }
            )
    return manifests if manifests else [_synthetic_manifest()]


def _synthetic_manifest() -> dict:
    return {
        "Run ID": "20260404_184419",
        "Source File": "fraudTrain.csv",
        "Total Rows": 1_296_675,
        "Total Batches": 1_297,
        "Failed Batches": 0,
        "Batch Size": 1_000,
    }


# ── Customer risk CSV ─────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_customer_risk() -> pd.DataFrame:
    path = ROOT / "tableau" / "data" / "tableau_customer_risk.csv"
    if path.exists():
        return pd.read_csv(path, low_memory=False)
    return _synthetic_customer_risk()


def _synthetic_customer_risk() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    n = 300
    risk_scores = rng.exponential(18, n).clip(0, 100)
    tiers = pd.cut(
        risk_scores,
        bins=[-1, 10, 30, 60, 101],
        labels=["Low", "Medium", "High", "Critical"],
    )
    states = ["CA", "TX", "NY", "FL", "IL", "OH", "GA", "NC", "MI", "PA",
              "AZ", "WA", "CO", "MA", "TN", "MO", "IN", "MD", "WI", "MN"]
    return pd.DataFrame(
        {
            "cc_num": rng.integers(int(1e15), int(9e15), n),
            "first": [f"User{i}" for i in range(n)],
            "last": [f"Sample{i}" for i in range(n)],
            "gender": rng.choice(["M", "F"], n),
            "state": rng.choice(states, n),
            "city": [f"City{rng.integers(1, 60)}" for _ in range(n)],
            "age": rng.integers(18, 80, n),
            "age_group": rng.choice(["18-24", "25-34", "35-44", "45-54", "55-64", "65+"], n),
            "total_transactions": rng.integers(50, 600, n),
            "avg_transaction_amount": rng.uniform(20, 200, n).round(2),
            "max_single_transaction": rng.uniform(100, 5000, n).round(2),
            "total_spend": rng.uniform(5_000, 120_000, n).round(2),
            "fraud_count": rng.integers(0, 6, n),
            "fraud_rate": rng.exponential(0.4, n).clip(0, 30).round(2),
            "late_night_ratio": rng.uniform(0, 0.3, n).round(3),
            "weekend_ratio": rng.uniform(0.2, 0.5, n).round(3),
            "unique_merchants": rng.integers(5, 60, n),
            "unique_categories": rng.integers(1, 14, n),
            "avg_distance_km": rng.uniform(5, 152, n).round(2),
            "active_days": rng.integers(30, 730, n),
            "risk_score": risk_scores.round(1),
            "risk_tier": tiers,
        }
    )


# ── Merchant analysis CSV ─────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_merchant_analysis() -> pd.DataFrame:
    path = ROOT / "tableau" / "data" / "tableau_merchant_analysis.csv"
    if path.exists():
        return pd.read_csv(path, low_memory=False)
    return _synthetic_merchant_data()


def _synthetic_merchant_data() -> pd.DataFrame:
    rng = np.random.default_rng(42)
    categories = [
        "gas_transport", "grocery_pos", "home", "shopping_pos", "kids_pets",
        "shopping_net", "entertainment", "food_dining", "personal_care",
        "health_fitness", "misc_pos", "misc_net", "grocery_net", "travel",
    ]
    n = 600
    return pd.DataFrame(
        {
            "merchant": [f"Merchant_{rng.integers(1, 120)}" for _ in range(n)],
            "category": rng.choice(categories, n),
            "transaction_date": pd.date_range("2019-01-01", periods=n, freq="D")
            .strftime("%Y-%m-%d")
            .tolist(),
            "total_transactions": rng.integers(1, 60, n),
            "total_amount": rng.uniform(50, 6_000, n).round(2),
            "avg_amount": rng.uniform(15, 200, n).round(2),
            "fraud_count": rng.integers(0, 5, n),
            "legit_count": rng.integers(5, 55, n),
            "fraud_rate": rng.exponential(0.8, n).clip(0, 25).round(2),
            "unique_customers": rng.integers(1, 40, n),
            "avg_distance_km": rng.uniform(10, 150, n).round(2),
            "txn_year": rng.choice([2019, 2020], n),
            "txn_month": rng.integers(1, 13, n),
            "txn_month_name": rng.choice(
                ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                 "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], n
            ),
        }
    )
