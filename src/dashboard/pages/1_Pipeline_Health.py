"""Page 1 — Pipeline Health: ingestion status, job metrics, data freshness."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from utils.data_loader import load_manifests, load_quality_reports

st.set_page_config(page_title="Pipeline Health", page_icon="🔧", layout="wide")
st.title("🔧 Pipeline Health")
st.caption("Ingestion status, Glue job results, and data freshness")

reports = load_quality_reports()
manifests = load_manifests()
latest = reports[-1]
rc = latest["record_counts"]

# ── Top KPIs ──────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Records In (Raw)", f"{rc['raw_input']:,}")
c2.metric("Records Out (Staging)", f"{rc['final_output']:,}")
c3.metric("Quality Score", f"{rc['quality_score_pct']:.1f}%",
          "✅ Above 95% threshold" if rc['quality_score_pct'] >= 95 else "⚠️ Below threshold")
c4.metric("Records Dropped", rc["null_dropped"] + rc["schema_failures"] + rc["duplicates_removed"],
          delta_color="inverse")

st.divider()

# ── Data Freshness ────────────────────────────────────────────────────────────
ts_raw = latest.get("report_timestamp", "")
if ts_raw:
    run_dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
    now_utc = datetime.now(tz=timezone.utc)
    age_hours = (now_utc - run_dt).total_seconds() / 3600
    freshness_color = "normal" if age_hours < 48 else "off"
    st.info(
        f"**Last pipeline run:** {run_dt.strftime('%Y-%m-%d %H:%M UTC')}  "
        f"({age_hours:.0f} hours ago)"
    )

# ── Quality drop-through funnel ───────────────────────────────────────────────
st.subheader("Quality Drop-Through Funnel")
funnel_stages = ["Raw Input", "After Schema Check", "After Null Drop", "After Dedup", "Final Output"]
funnel_values = [
    rc["raw_input"],
    rc["raw_input"] - rc["schema_failures"],
    rc["raw_input"] - rc["schema_failures"] - rc["null_dropped"],
    rc["raw_input"] - rc["schema_failures"] - rc["null_dropped"] - rc["duplicates_removed"],
    rc["final_output"],
]

fig_funnel = go.Figure(
    go.Funnel(
        y=funnel_stages,
        x=funnel_values,
        texttemplate="%{value:,.0f}",
        connector={"line": {"color": "#2CA6A4", "width": 2}},
        marker={"color": ["#2CA6A4", "#27AE60", "#27AE60", "#27AE60", "#27AE60"]},
    )
)
fig_funnel.update_layout(
    height=280,
    margin=dict(l=0, r=0, t=10, b=10),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font_color="#FAFAFA",
)
st.plotly_chart(fig_funnel, use_container_width=True)

# ── Ingestion Runs Table ──────────────────────────────────────────────────────
st.subheader("Ingestion Runs")
runs_df = pd.DataFrame(manifests)
total_rows = runs_df["Total Rows"].sum()
total_batches = runs_df["Total Batches"].sum()
total_failed = runs_df["Failed Batches"].sum()

c1, c2, c3 = st.columns(3)
c1.metric("Total Rows Ingested", f"{total_rows:,}")
c2.metric("Total Batches", f"{total_batches:,}")
c3.metric("Failed Batches", int(total_failed), delta_color="inverse")

st.dataframe(
    runs_df.style.highlight_max(subset=["Total Rows"], color="#1e3d2f"),
    use_container_width=True,
    hide_index=True,
)

# ── Glue Job Summary ──────────────────────────────────────────────────────────
st.subheader("Glue Job Status")
glue_jobs = pd.DataFrame(
    [
        {
            "Job Name": "raw-to-staging",
            "Description": "Schema enforce, dedup, enrich (Bronze→Silver)",
            "Status": "✅ Succeeded",
            "Input": "1,852,394 rows",
            "Output": "1,852,394 rows",
            "Quality": "100.0%",
        },
        {
            "Job Name": "staging-to-analytics",
            "Description": "Merchant summaries, risk scores, anomaly flags (Silver→Gold)",
            "Status": "✅ Succeeded",
            "Input": "1,852,394 rows",
            "Output": "4 tables",
            "Quality": "—",
        },
        {
            "Job Name": "fraud-feature-engineering",
            "Description": "Lag/velocity/geo ML features (Silver→Gold)",
            "Status": "✅ Succeeded",
            "Input": "1,852,394 rows",
            "Output": "Feature table",
            "Quality": "—",
        },
        {
            "Job Name": "export-for-tableau",
            "Description": "Denormalized CSVs for Tableau Public (Gold→Exports)",
            "Status": "✅ Succeeded",
            "Input": "4 tables",
            "Output": "3 CSV files",
            "Quality": "—",
        },
    ]
)
st.dataframe(glue_jobs, use_container_width=True, hide_index=True)

# ── CloudWatch Metrics Reference ──────────────────────────────────────────────
with st.expander("CloudWatch Metrics Published (Namespace: CreditTxnPipeline)"):
    metrics_df = pd.DataFrame(
        [
            {"Metric": "BatchIngestionCount", "Unit": "Count", "Description": "Rows ingested per batch"},
            {"Metric": "BatchCount", "Unit": "Count", "Description": "Total batches processed"},
            {"Metric": "FailedBatchCount", "Unit": "Count", "Description": "Batches that failed"},
            {"Metric": "DataQualityScore", "Unit": "Percent", "Description": "% of records passing all checks"},
            {"Metric": "RecordsDropped", "Unit": "Count", "Description": "Rows removed during quality checks"},
            {"Metric": "DuplicatesRemoved", "Unit": "Count", "Description": "Deduplicated records"},
            {"Metric": "AnomalyDetectionCount", "Unit": "Count", "Description": "Transactions flagged per run"},
            {"Metric": "AnomalyTruePositives", "Unit": "Count", "Description": "Flagged transactions that were fraud"},
            {"Metric": "AnomalyPrecision", "Unit": "Percent", "Description": "TP / Total Flagged"},
            {"Metric": "AnomalyRecall", "Unit": "Percent", "Description": "TP / Total Fraud"},
            {"Metric": "GlueJobDuration", "Unit": "Seconds", "Description": "Execution time per job"},
            {"Metric": "GlueJobSuccess", "Unit": "Count (0/1)", "Description": "Job success indicator"},
        ]
    )
    st.dataframe(metrics_df, use_container_width=True, hide_index=True)
