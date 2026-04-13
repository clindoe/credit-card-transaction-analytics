"""Page 3 — Anomaly Explorer: risk tiers, fraud flags, searchable customer table."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.data_loader import load_customer_risk, load_merchant_analysis

st.set_page_config(page_title="Anomaly Explorer", page_icon="🚨", layout="wide")
st.title("🚨 Anomaly Explorer")
st.caption("Customer risk tiers, anomaly accuracy metrics, and searchable fraud flags")

customers = load_customer_risk()
merchants = load_merchant_analysis()

# Normalise column names (handle CSV vs synthetic)
customers.columns = [c.strip() for c in customers.columns]

TIER_COLORS = {
    "Critical": "#E74C3C",
    "High": "#E67E22",
    "Medium": "#F1C40F",
    "Low": "#27AE60",
}
TIER_ORDER = ["Critical", "High", "Medium", "Low"]

# ── Anomaly Detection Accuracy KPIs (from known run metrics) ─────────────────
st.subheader("Anomaly Detection Accuracy")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Anomalies Flagged", "~125,000", "6.7% of transactions")
c2.metric("True Positives (Fraud)", "~2,400", "caught by flags")
c3.metric("Precision", "1.9%", help="TP / (TP + FP)")
c4.metric("Recall", "24.8%", help="TP / Total Fraud — catches 1 in 4 fraud cases")

with st.expander("Anomaly Flag Types"):
    flags_df = pd.DataFrame(
        [
            {"Flag": "flag_amount_outlier", "Rule": "Amount > 3σ from customer mean",
             "Coverage": "High precision for large deviations"},
            {"Flag": "flag_unusual_hour", "Rule": "Transaction at 2–5 AM",
             "Coverage": "Catches late-night fraud patterns"},
            {"Flag": "flag_geographic_outlier", "Rule": "Distance > 3σ from customer home baseline",
             "Coverage": "Detects card-not-present / travel fraud"},
            {"Flag": "flag_high_value", "Rule": "Amount > $500",
             "Coverage": "Simple threshold — high false-positive rate"},
        ]
    )
    st.dataframe(flags_df, use_container_width=True, hide_index=True)

st.divider()

# ── Sidebar Filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")
    selected_tiers = st.multiselect(
        "Risk Tier", TIER_ORDER, default=TIER_ORDER
    )
    if "state" in customers.columns:
        all_states = sorted(customers["state"].dropna().unique().tolist())
        selected_states = st.multiselect("State", all_states, default=[])
    else:
        selected_states = []

    min_risk = st.slider("Min Risk Score", 0, 100, 0)
    search_name = st.text_input("Search by Name", "")

# ── Apply filters ─────────────────────────────────────────────────────────────
filtered = customers.copy()
if "risk_tier" in filtered.columns:
    filtered = filtered[filtered["risk_tier"].astype(str).isin(selected_tiers)]
if selected_states and "state" in filtered.columns:
    filtered = filtered[filtered["state"].isin(selected_states)]
if "risk_score" in filtered.columns:
    filtered = filtered[filtered["risk_score"] >= min_risk]
if search_name and "first" in filtered.columns:
    mask = (
        filtered["first"].str.contains(search_name, case=False, na=False)
        | filtered["last"].str.contains(search_name, case=False, na=False)
    )
    filtered = filtered[mask]

# ── Customer Risk KPIs ────────────────────────────────────────────────────────
st.subheader(f"Customer Risk Summary ({len(filtered):,} customers)")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Customers", f"{len(customers):,}")
if "risk_tier" in customers.columns:
    critical_n = (customers["risk_tier"].astype(str) == "Critical").sum()
    high_n = (customers["risk_tier"].astype(str) == "High").sum()
    c2.metric("Critical Risk", f"{critical_n:,}")
    c3.metric("High Risk", f"{high_n:,}")
if "risk_score" in customers.columns:
    c4.metric("Avg Risk Score", f"{customers['risk_score'].mean():.1f}/100")

# ── Risk Tier Distribution ────────────────────────────────────────────────────
col_pie, col_hist = st.columns(2)

with col_pie:
    st.subheader("Risk Tier Distribution")
    if "risk_tier" in customers.columns:
        tier_counts = (
            customers["risk_tier"].astype(str).value_counts().reindex(TIER_ORDER, fill_value=0).reset_index()
        )
        tier_counts.columns = ["Tier", "Count"]
        fig_pie = px.pie(
            tier_counts, names="Tier", values="Count",
            color="Tier",
            color_discrete_map=TIER_COLORS,
            hole=0.45,
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        fig_pie.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", font_color="#FAFAFA",
            height=300, showlegend=False,
        )
        st.plotly_chart(fig_pie, use_container_width=True)

with col_hist:
    st.subheader("Risk Score Distribution")
    if "risk_score" in customers.columns:
        fig_hist = px.histogram(
            customers, x="risk_score", nbins=40,
            color_discrete_sequence=["#2CA6A4"],
            labels={"risk_score": "Risk Score", "count": "Customers"},
        )
        fig_hist.add_vline(x=10, line_dash="dash", line_color="#F1C40F",
                           annotation_text="Medium", annotation_position="top right")
        fig_hist.add_vline(x=30, line_dash="dash", line_color="#E67E22",
                           annotation_text="High", annotation_position="top right")
        fig_hist.add_vline(x=60, line_dash="dash", line_color="#E74C3C",
                           annotation_text="Critical", annotation_position="top right")
        fig_hist.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#FAFAFA", height=300,
        )
        st.plotly_chart(fig_hist, use_container_width=True)

st.divider()

# ── Late Night vs Risk Scatter ────────────────────────────────────────────────
if {"late_night_ratio", "risk_score", "risk_tier", "avg_transaction_amount"}.issubset(customers.columns):
    st.subheader("Late-Night Activity vs Risk Score")
    hover_cols = [c for c in ["first", "last", "state", "fraud_rate", "total_transactions"] if c in customers.columns]
    fig_scatter = px.scatter(
        customers,
        x="late_night_ratio",
        y="risk_score",
        color="risk_tier",
        size="avg_transaction_amount",
        size_max=18,
        color_discrete_map=TIER_COLORS,
        category_orders={"risk_tier": TIER_ORDER},
        hover_data=hover_cols,
        labels={
            "late_night_ratio": "Late-Night Ratio (2–5 AM)",
            "risk_score": "Risk Score",
            "risk_tier": "Risk Tier",
        },
    )
    fig_scatter.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color="#FAFAFA", height=380,
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

# ── Fraud Rate by Category (from merchant data) ───────────────────────────────
if "category" in merchants.columns and "fraud_rate" in merchants.columns:
    st.subheader("Average Fraud Rate by Category")
    cat_fraud = (
        merchants.groupby("category")["fraud_rate"]
        .mean()
        .reset_index()
        .sort_values("fraud_rate", ascending=True)
        .rename(columns={"category": "Category", "fraud_rate": "Avg Fraud Rate (%)"})
    )
    cat_fraud["Category"] = cat_fraud["Category"].str.replace("_", " ").str.title()
    fig_cat = px.bar(
        cat_fraud, x="Avg Fraud Rate (%)", y="Category",
        orientation="h",
        color="Avg Fraud Rate (%)",
        color_continuous_scale=["#1a3d2b", "#E74C3C"],
    )
    fig_cat.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font_color="#FAFAFA", height=400, coloraxis_showscale=False,
    )
    st.plotly_chart(fig_cat, use_container_width=True)

st.divider()

# ── Searchable Customer Table ─────────────────────────────────────────────────
st.subheader("Customer Risk Table")
display_cols = [c for c in [
    "first", "last", "state", "age_group", "risk_tier", "risk_score",
    "fraud_rate", "total_transactions", "avg_transaction_amount",
    "late_night_ratio", "unique_categories", "avg_distance_km",
] if c in filtered.columns]

if display_cols:
    display_df = filtered[display_cols].copy()
    if "risk_tier" in display_df.columns:
        display_df = display_df.sort_values("risk_score", ascending=False)

    st.dataframe(
        display_df.head(500),
        use_container_width=True,
        hide_index=True,
    )
    st.caption(f"Showing top 500 of {len(filtered):,} filtered customers. Adjust filters in sidebar to narrow results.")
else:
    st.info("No columns available to display.")

# ── Top High-Risk Customers ───────────────────────────────────────────────────
if "risk_score" in customers.columns and "risk_tier" in customers.columns:
    with st.expander("Top 20 Highest-Risk Customers"):
        top_risk_cols = [c for c in ["first", "last", "state", "risk_tier", "risk_score",
                                      "fraud_count", "fraud_rate", "total_spend"] if c in customers.columns]
        top20 = customers.nlargest(20, "risk_score")[top_risk_cols]
        st.dataframe(top20, use_container_width=True, hide_index=True)
