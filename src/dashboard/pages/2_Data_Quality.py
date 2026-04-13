"""Page 2 — Data Quality Deep Dive: null rates, distributions, quarantine viewer."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.data_loader import load_quality_reports

st.set_page_config(page_title="Data Quality", page_icon="🔍", layout="wide")
st.title("🔍 Data Quality Deep Dive")
st.caption("Null rates, duplicate detection, amount distributions, and schema compliance")

reports = load_quality_reports()
latest = reports[-1]
rc = latest["record_counts"]
amount_stats = latest.get("amount_statistics", {})
category_dist = latest.get("category_distribution", {})
bucket_dist = latest.get("amount_bucket_distribution", {})
fraud_dist = latest.get("fraud_distribution", {})
dist_stats = latest.get("distance_statistics", {})
null_counts = latest.get("null_counts_per_column", {})

# ── Quality Score Gauge ───────────────────────────────────────────────────────
col_gauge, col_stats = st.columns([1, 2])

with col_gauge:
    st.subheader("Quality Score")
    score = rc["quality_score_pct"]
    color = "#27AE60" if score >= 95 else "#E67E22" if score >= 80 else "#E74C3C"

    fig_gauge = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=score,
            number={"suffix": "%", "font": {"size": 40, "color": "#FAFAFA"}},
            gauge={
                "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "#FAFAFA"},
                "bar": {"color": color},
                "bgcolor": "#1E2329",
                "borderwidth": 2,
                "bordercolor": "#2CA6A4",
                "steps": [
                    {"range": [0, 80], "color": "#3d1a1a"},
                    {"range": [80, 95], "color": "#3d2e0a"},
                    {"range": [95, 100], "color": "#1a3d2b"},
                ],
                "threshold": {
                    "line": {"color": "#E74C3C", "width": 3},
                    "thickness": 0.75,
                    "value": 95,
                },
            },
        )
    )
    fig_gauge.update_layout(
        height=250,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        font_color="#FAFAFA",
    )
    st.plotly_chart(fig_gauge, use_container_width=True)
    threshold_label = "✅ Above 95% threshold" if score >= 95 else "⚠️ Below 95% threshold"
    st.caption(threshold_label)

with col_stats:
    st.subheader("Record Counts")
    c1, c2 = st.columns(2)
    c1.metric("Raw Input", f"{rc['raw_input']:,}")
    c1.metric("Final Output", f"{rc['final_output']:,}")
    c2.metric("Schema Failures", rc["schema_failures"], delta_color="inverse")
    c2.metric("Null Dropped", rc["null_dropped"], delta_color="inverse")
    c1.metric("Duplicates Removed", rc["duplicates_removed"], delta_color="inverse")

    fraud_total = fraud_dist.get("1", 0)
    legit_total = fraud_dist.get("0", 0)
    total = fraud_total + legit_total
    c2.metric("Fraud Rate", f"{fraud_total/total*100:.2f}%" if total else "N/A")

st.divider()

# ── Null Rates ────────────────────────────────────────────────────────────────
st.subheader("Null Rates by Column")
if null_counts:
    null_df = pd.DataFrame(
        [{"Column": k, "Null Count": v, "Null Rate (%)": round(v / rc["raw_input"] * 100, 4)}
         for k, v in null_counts.items() if v > 0]
    ).sort_values("Null Rate (%)", ascending=False)
    if not null_df.empty:
        fig_null = px.bar(
            null_df, x="Column", y="Null Rate (%)",
            color="Null Rate (%)", color_continuous_scale=["#27AE60", "#E74C3C"],
            title="Null Rate by Column",
        )
        fig_null.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#FAFAFA", height=300,
        )
        st.plotly_chart(fig_null, use_container_width=True)
    else:
        st.success("✅ Zero null values detected in any column — dataset is complete.")
else:
    st.success("✅ Zero null values detected in any column — dataset is complete.")

# ── Amount Statistics ─────────────────────────────────────────────────────────
col_amt, col_bucket = st.columns(2)

with col_amt:
    st.subheader("Transaction Amount Statistics")
    if amount_stats:
        amt_df = pd.DataFrame(
            [
                {"Statistic": "Minimum", "Value ($)": f"${amount_stats['min']:,.2f}"},
                {"Statistic": "Median", "Value ($)": f"${amount_stats['median']:,.2f}"},
                {"Statistic": "Mean", "Value ($)": f"${amount_stats['mean']:,.2f}"},
                {"Statistic": "Std Dev", "Value ($)": f"${amount_stats['stddev']:,.2f}"},
                {"Statistic": "Maximum", "Value ($)": f"${amount_stats['max']:,.2f}"},
            ]
        )
        st.dataframe(amt_df, use_container_width=True, hide_index=True)

        dist_df = pd.DataFrame(
            [
                {"Stat": "Min (km)", "Value": dist_stats.get("min_km", "—")},
                {"Stat": "Mean (km)", "Value": dist_stats.get("mean_km", "—")},
                {"Stat": "Max (km)", "Value": dist_stats.get("max_km", "—")},
            ]
        )
        st.caption("Distance from Home")
        st.dataframe(dist_df, use_container_width=True, hide_index=True)

with col_bucket:
    st.subheader("Amount Bucket Distribution")
    if bucket_dist:
        bucket_order = ["under_10", "10_to_50", "50_to_200", "200_to_1000", "over_1000"]
        bucket_labels = ["< $10", "$10–$50", "$50–$200", "$200–$1K", "> $1K"]
        bucket_vals = [bucket_dist.get(k, 0) for k in bucket_order]

        fig_bucket = px.bar(
            x=bucket_vals, y=bucket_labels,
            orientation="h",
            labels={"x": "Transactions", "y": "Amount Range"},
            color=bucket_vals,
            color_continuous_scale=["#2CA6A4", "#27AE60"],
        )
        fig_bucket.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#FAFAFA", height=280, showlegend=False,
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig_bucket, use_container_width=True)

st.divider()

# ── Category Distribution ─────────────────────────────────────────────────────
st.subheader("Transaction Volume by Category")
if category_dist:
    cat_df = (
        pd.DataFrame(
            [{"Category": k.replace("_", " ").title(), "Transactions": v}
             for k, v in category_dist.items()]
        )
        .sort_values("Transactions", ascending=True)
    )
    fig_cat = px.bar(
        cat_df, x="Transactions", y="Category",
        orientation="h",
        color="Transactions",
        color_continuous_scale=["#1a3d5c", "#2CA6A4"],
        labels={"Transactions": "Transaction Count"},
    )
    fig_cat.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color="#FAFAFA", height=400, coloraxis_showscale=False,
    )
    st.plotly_chart(fig_cat, use_container_width=True)

# ── Fraud vs Legitimate ───────────────────────────────────────────────────────
st.subheader("Fraud vs Legitimate Split")
col_pie, col_txt = st.columns([1, 2])
with col_pie:
    if fraud_dist:
        labels = ["Legitimate", "Fraudulent"]
        values = [fraud_dist.get("0", 0), fraud_dist.get("1", 0)]
        fig_pie = px.pie(
            names=labels, values=values,
            color=labels,
            color_discrete_map={"Legitimate": "#BDC3C7", "Fraudulent": "#E74C3C"},
            hole=0.5,
        )
        fig_pie.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
            height=280, showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.2),
        )
        st.plotly_chart(fig_pie, use_container_width=True)

with col_txt:
    if fraud_dist:
        fraud_n = fraud_dist.get("1", 0)
        legit_n = fraud_dist.get("0", 0)
        total_n = fraud_n + legit_n
        st.metric("Fraudulent Transactions", f"{fraud_n:,}", f"{fraud_n/total_n*100:.2f}%")
        st.metric("Legitimate Transactions", f"{legit_n:,}", f"{legit_n/total_n*100:.2f}%")
        st.caption(
            "Fraud rate of **0.52%** is consistent with real-world card fraud prevalence. "
            "The imbalanced dataset requires careful threshold tuning for anomaly detection."
        )

# ── Quarantine Viewer ─────────────────────────────────────────────────────────
with st.expander("Quarantine Zone Viewer"):
    q_count = rc["schema_failures"] + rc["null_dropped"]
    if q_count == 0:
        st.success(
            "✅ No records quarantined in the latest run. All 1,852,394 records "
            "passed schema validation and null checks."
        )
    else:
        st.warning(f"⚠️ {q_count:,} records quarantined. "
                   f"Check `s3://credit-txn-analytics-adil/staging/quarantine/`")
