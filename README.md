# Credit Card Transaction Analytics Pipeline

A production-grade ELT pipeline on AWS that ingests 1.3M+ credit card transactions, detects fraud anomalies, and serves business intelligence dashboards.

## Architecture
[Transaction Simulator (Python)] -> [AWS S3 - Raw Zone] -> [AWS Glue (PySpark ETL Jobs)]

## Tech Stack
- **Cloud:** AWS (S3, Glue, Athena, CloudWatch, SNS)
- **Processing:** PySpark, Python, Pandas, PyArrow
- **BI:** Tableau Public, Streamlit
- **Data:** 1.3M transactions from Kaggle
- **Infrastructure:** CloudFormation (IaC), GitHub Actions (CI/CD)

## Project Status
- [x] Day 1: AWS setup + data ingestion
- [ ] Day 2: Glue setup + raw → staging ETL
- [ ] Day 3: Analytics aggregations + feature engineering
- [ ] Day 4: Athena SQL queries + monitoring
- [ ] Day 5: Tableau dashboards
- [ ] Day 6: Streamlit dashboard + IaC
- [ ] Day 7: Documentation + polish

## Quick Start
```bash
git clone https://github.com/mdajmainadil/credit-card-transaction-analytics.git
cd credit-card-transaction-analytics
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
