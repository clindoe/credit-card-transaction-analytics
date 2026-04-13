# Credit Card Transaction Analytics Pipeline

A production-grade ELT pipeline on AWS that ingests 1.85M credit card transactions, detects fraud anomalies, and serves business intelligence dashboards.

## Live Dashboard

**Streamlit:** [credit-card-transaction-analytics.streamlit.app](https://credit-card-transaction-analytics.streamlit.app/)

**Tableau Public:** [Fraud Detection Dashboards](https://public.tableau.com)

## Architecture

```
CSV (Kaggle 1.85M rows)
        │
        ▼
[Python Ingestion] ──▶ S3 raw/         (Bronze — Parquet, partitioned by date)
                              │
                     Glue: raw-to-staging
                              │
                        S3 staging/     (Silver — cleaned, enriched, deduped)
                              │
               ┌──────────────┼──────────────────────┐
               │                                      │
      Glue: staging-to-analytics          Glue: fraud-feature-engineering
               │                                      │
         S3 analytics/  (Gold)               fraud_feature_table (ML-ready)
         ├── daily_merchant_summary
         ├── customer_risk_scores
         ├── category_spending_trends
         └── fraud_detection_results
               │
      Glue: export-for-tableau
               │
         S3 exports/ ──▶ Tableau Public + Streamlit Dashboard
```

## Tech Stack

| Layer | Tools |
|---|---|
| Cloud | AWS S3, Glue (PySpark 4.0), Athena, CloudWatch, SNS |
| Processing | PySpark, Python, Pandas, PyArrow |
| Dashboards | Streamlit, Tableau Public, Plotly |
| IaC | CloudFormation (4 stacks) |
| CI/CD | GitHub Actions (lint + test + CFN validate) |
| Data | 1.85M transactions — Kaggle Credit Card Fraud Detection |

## Key Metrics

- **1,852,394** transactions processed end-to-end
- **100%** data quality score (zero nulls, zero schema failures, zero duplicates)
- **0.52%** fraud rate (9,651 fraudulent transactions)
- **6 analytics tables** across the Gold layer
- **12+ CloudWatch metrics** published per pipeline run
- **69 unit tests**, all passing

## Project Status

- [x] Day 1: AWS setup + data ingestion (S3, Python batch uploader)
- [x] Day 2: Glue setup + raw → staging ETL (Bronze → Silver)
- [x] Day 3: Analytics aggregations + fraud feature engineering (Silver → Gold)
- [x] Day 4: Athena SQL suite (15 queries) + CloudWatch monitoring + SNS alerting
- [x] Day 5: Tableau Public dashboards (3 views + story)
- [x] Day 6: Streamlit dashboard + CloudFormation IaC + GitHub Actions CI
- [ ] Day 7: Documentation + polish

## Quick Start

```bash
git clone https://github.com/clindoe/credit-card-transaction-analytics.git
cd credit-card-transaction-analytics
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run Streamlit dashboard locally
streamlit run src/dashboard/app.py
```

## One-Command Infrastructure Deploy

```bash
# Deploy all AWS infrastructure (IAM → S3 → Glue → Monitoring)
ALERT_EMAIL=you@email.com ./setup.sh

# Dry-run (validate CloudFormation templates only)
./setup.sh --dry-run
```

## Run Tests

```bash
python -m pytest tests/ -v
```

## Repository Structure

```
├── src/
│   ├── ingestion/          # Python batch uploader (CSV → Parquet → S3)
│   ├── glue_jobs/          # 4 PySpark ETL jobs
│   ├── athena_queries/     # 15 Athena SQL queries
│   ├── monitoring/         # CloudWatch metrics + SNS alerting
│   └── dashboard/          # Streamlit app (3 pages)
├── infrastructure/
│   └── cloudformation/     # 4 CloudFormation templates
├── tableau/                # Tableau export CSVs + README
├── tests/                  # pytest unit tests (69 tests)
├── setup.sh                # One-command deployment script
└── .github/workflows/      # GitHub Actions CI pipeline
```
