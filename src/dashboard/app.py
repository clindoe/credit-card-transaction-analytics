"""
Credit Card Transaction Analytics — Streamlit Dashboard
Entry point: streamlit run src/dashboard/app.py
"""

import pandas as pd
import streamlit as st

from utils.data_loader import load_quality_reports, load_manifests

st.set_page_config(
    page_title="CC Fraud Analytics",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.shields.io/badge/AWS-Glue-orange?logo=amazonaws", use_container_width=False)
    st.markdown("### Navigation")
    st.markdown(
        """
- 🔧 **Pipeline Health** — job status, ingestion KPIs, freshness
- 🔍 **Data Quality** — null rates, distributions, quality score
- 🚨 **Anomaly Explorer** — risk tiers, anomaly flags, customer search
        """
    )
    st.divider()
    st.caption("Data Lake: `s3://credit-txn-analytics-adil`")
    st.caption("Namespace: `CreditTxnPipeline`")

# ── Hero ──────────────────────────────────────────────────────────────────────
st.title("💳 Credit Card Transaction Analytics")
st.markdown(
    "End-to-end fraud detection pipeline — **1.85 M transactions** through a "
    "medallion data lake on AWS (S3 · Glue · Athena · CloudWatch)."
)

# ── Pipeline summary KPIs ─────────────────────────────────────────────────────
reports = load_quality_reports()
manifests = load_manifests()
latest = reports[-1]
rc = latest["record_counts"]
fraud_dist = latest.get("fraud_distribution", {})
total_fraud = fraud_dist.get("1", 9_651)
total_legit = fraud_dist.get("0", 1_842_743)
total = rc["raw_input"]
fraud_rate = round(total_fraud / total * 100, 2) if total else 0

total_rows_ingested = sum(m["Total Rows"] for m in manifests)
total_batches = sum(m["Total Batches"] for m in manifests)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Records Processed", f"{total:,}")
c2.metric("Quality Score", f"{rc['quality_score_pct']:.1f}%")
c3.metric("Fraud Cases", f"{total_fraud:,}", f"{fraud_rate}% rate")
c4.metric("Total Batches Ingested", f"{total_batches:,}")
c5.metric("Failed Batches", sum(m["Failed Batches"] for m in manifests), delta_color="inverse")

st.divider()

# ── Architecture overview ─────────────────────────────────────────────────────
col_left, col_right = st.columns([3, 2])

with col_left:
    st.subheader("Pipeline Architecture")
    st.markdown(
        """
```
CSV (Kaggle)  ──▶  S3 raw/          [Bronze]  1,852,394 rows
                        │
                   Glue: raw-to-staging
                        │
                   S3 staging/        [Silver]  ✅ 100% quality score
                        │
          ┌─────────────┼─────────────────────────┐
          │             │                         │
     Glue: staging-to-analytics         Glue: feature-engineering
          │                                       │
   S3 analytics/  [Gold]                   fraud_feature_table
   ├── daily_merchant_summary
   ├── customer_risk_scores
   ├── category_spending_trends
   └── fraud_detection_results
          │
   Glue: export-for-tableau
          │
   S3 exports/  ──▶  Tableau Public  ──▶  Streamlit Dashboard
```
        """
    )

with col_right:
    st.subheader("Glue Job Status")
    jobs_df = pd.DataFrame(
        [
            {"Job": "raw-to-staging", "Layer": "Bronze→Silver",
             "Status": "✅ Succeeded", "Output": "1,852,394 rows"},
            {"Job": "staging-to-analytics", "Layer": "Silver→Gold",
             "Status": "✅ Succeeded", "Output": "4 analytics tables"},
            {"Job": "fraud-feature-engineering", "Layer": "Silver→Gold",
             "Status": "✅ Succeeded", "Output": "ML feature table"},
            {"Job": "export-for-tableau", "Layer": "Gold→Exports",
             "Status": "✅ Succeeded", "Output": "3 CSV exports"},
        ]
    )
    st.dataframe(jobs_df, use_container_width=True, hide_index=True)

    st.subheader("Data Lake Zones")
    zones_df = pd.DataFrame(
        [
            {"Zone": "Raw (Bronze)", "Prefix": "raw/transactions/", "Format": "Parquet"},
            {"Zone": "Staging (Silver)", "Prefix": "staging/transactions_cleaned/", "Format": "Parquet"},
            {"Zone": "Analytics (Gold)", "Prefix": "analytics/*/", "Format": "Parquet"},
            {"Zone": "Exports", "Prefix": "exports/", "Format": "CSV"},
        ]
    )
    st.dataframe(zones_df, use_container_width=True, hide_index=True)

st.divider()
st.caption("Use the sidebar to navigate between dashboard pages.")
