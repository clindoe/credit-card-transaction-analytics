"""
export_for_tableau.py — AWS Glue PySpark Job
Reads gold-zone Parquet tables and exports denormalized CSVs for Tableau Public.

Produces three files:
1. tableau_fraud_overview.csv — daily fraud metrics, category breakdowns, hourly patterns
2. tableau_customer_risk.csv — customer-level risk scores with demographics
3. tableau_merchant_analysis.csv — merchant performance, fraud rates, geographic data

Why CSV: Tableau Public cannot connect to Athena/S3 directly. It needs local files.
Why denormalized: flat CSVs load faster in Tableau than needing complex joins.
"""

import sys
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
BUCKET = "credit-txn-analytics-adil"
STAGING_PATH = f"s3://{BUCKET}/staging/transactions_cleaned/"
MERCHANT_SUMMARY_PATH = f"s3://{BUCKET}/analytics/daily_merchant_summary/"
CUSTOMER_RISK_PATH = f"s3://{BUCKET}/analytics/customer_risk_scores/"
CATEGORY_TRENDS_PATH = f"s3://{BUCKET}/analytics/category_spending_trends/"
ANOMALY_PATH = f"s3://{BUCKET}/analytics/fraud_detection_results/"
EXPORTS_PATH = f"s3://{BUCKET}/exports"

# ──────────────────────────────────────────────
# INITIALIZE
# ──────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()
logger.info("═══ Starting export_for_tableau job ═══")


def write_single_csv(df, path, filename):
    """
    Write a DataFrame as a single CSV file.

    Spark normally writes partitioned output (part-00000, etc).
    We coalesce to 1 partition to get a single file, then rename it.
    """
    temp_path = f"{path}/_temp_{filename}"

    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", "true") \
      .csv(temp_path)

    # Spark writes to a directory with part files — we need to find and rename
    import boto3
    s3 = boto3.client("s3")

    # List files in the temp directory
    prefix = temp_path.replace(f"s3://{BUCKET}/", "")
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

    # Find the actual CSV file (not _SUCCESS or _committed)
    csv_key = None
    for obj in response.get("Contents", []):
        if obj["Key"].endswith(".csv"):
            csv_key = obj["Key"]
            break

    if csv_key:
        # Copy to final location
        final_key = f"exports/{filename}"
        s3.copy_object(
            Bucket=BUCKET,
            CopySource={"Bucket": BUCKET, "Key": csv_key},
            Key=final_key
        )
        logger.info(f"Exported: s3://{BUCKET}/{final_key}")

        # Clean up temp directory
        for obj in response.get("Contents", []):
            s3.delete_object(Bucket=BUCKET, Key=obj["Key"])
        # Delete _SUCCESS file if present
        try:
            s3.delete_object(Bucket=BUCKET, Key=f"{prefix}/_SUCCESS")
        except Exception:
            pass
    else:
        logger.error(f"Could not find CSV file in temp output for {filename}")


# ══════════════════════════════════════════════
# EXPORT 1: FRAUD OVERVIEW
# ══════════════════════════════════════════════
logger.info("Building tableau_fraud_overview.csv...")

# Read staging for transaction-level detail
df_staging = spark.read.parquet(STAGING_PATH)

# Build a denormalized fraud overview table
# One row per transaction with all relevant fields for Tableau
fraud_overview = df_staging.select(
    "trans_num",
    "trans_date_trans_time",
    F.to_date(F.col("trans_date_trans_time")).alias("transaction_date"),
    "cc_num",
    "merchant",
    "category",
    "amt",
    "amount_bucket",
    "first",
    "last",
    "gender",
    "state",
    "city",
    "lat",
    "long",
    "merch_lat",
    "merch_long",
    "is_fraud",
    "transaction_hour",
    "day_of_week",
    "day_of_week_name",
    "is_weekend",
    "distance_from_home_km",
)

# Add formatted columns for Tableau
fraud_overview = fraud_overview.withColumn(
    "fraud_label",
    F.when(F.col("is_fraud") == 1, "Fraudulent").otherwise("Legitimate")
)

# Add hour bucket for heat map
fraud_overview = fraud_overview.withColumn(
    "hour_bucket",
    F.when(F.col("transaction_hour").between(0, 5), "Late Night (12-5AM)")
     .when(F.col("transaction_hour").between(6, 11), "Morning (6-11AM)")
     .when(F.col("transaction_hour").between(12, 17), "Afternoon (12-5PM)")
     .otherwise("Evening (6-11PM)")
)

write_single_csv(fraud_overview, EXPORTS_PATH, "tableau_fraud_overview.csv")
fraud_export_count = fraud_overview.count()
logger.info(f"Fraud overview export: {fraud_export_count:,} rows")


# ══════════════════════════════════════════════
# EXPORT 2: CUSTOMER RISK
# ══════════════════════════════════════════════
logger.info("Building tableau_customer_risk.csv...")

customer_risk = spark.read.parquet(CUSTOMER_RISK_PATH)

# Select and rename columns for Tableau readability
customer_export = customer_risk.select(
    "cc_num",
    "first",
    "last",
    "gender",
    "state",
    "city",
    "dob",
    "total_transactions",
    "avg_transaction_amount",
    "stddev_transaction_amount",
    "max_single_transaction",
    "min_single_transaction",
    "total_spend",
    "unique_merchants",
    "unique_categories",
    "first_transaction",
    "last_transaction",
    "active_days",
    "fraud_count",
    "fraud_rate",
    "weekend_transactions",
    "weekend_ratio",
    "late_night_transactions",
    "late_night_ratio",
    "txn_frequency",
    "avg_distance_km",
    "max_distance_km",
    "risk_score",
    "risk_tier",
)

# Add age for demographic analysis
customer_export = customer_export.withColumn(
    "age",
    F.floor(F.datediff(F.current_date(), F.to_date(F.col("dob"))) / 365.25)
)

# Add age group
customer_export = customer_export.withColumn(
    "age_group",
    F.when(F.col("age") < 25, "18-24")
     .when(F.col("age") < 35, "25-34")
     .when(F.col("age") < 45, "35-44")
     .when(F.col("age") < 55, "45-54")
     .when(F.col("age") < 65, "55-64")
     .otherwise("65+")
)

write_single_csv(customer_export, EXPORTS_PATH, "tableau_customer_risk.csv")
customer_export_count = customer_export.count()
logger.info(f"Customer risk export: {customer_export_count:,} rows")


# ══════════════════════════════════════════════
# EXPORT 3: MERCHANT ANALYSIS
# ══════════════════════════════════════════════
logger.info("Building tableau_merchant_analysis.csv...")

merchant_summary = spark.read.parquet(MERCHANT_SUMMARY_PATH)

merchant_export = merchant_summary.select(
    "merchant",
    "category",
    "transaction_date",
    "total_transactions",
    "total_amount",
    "avg_amount",
    "min_amount",
    "max_amount",
    "stddev_amount",
    "fraud_count",
    "legit_count",
    "fraud_rate",
    "unique_customers",
    "avg_distance_km",
)

# Add month and year as separate columns for Tableau filtering
merchant_export = merchant_export.withColumn(
    "txn_year", F.year(F.col("transaction_date"))
).withColumn(
    "txn_month", F.month(F.col("transaction_date"))
).withColumn(
    "txn_month_name", F.date_format(F.col("transaction_date"), "MMMM")
)

write_single_csv(merchant_export, EXPORTS_PATH, "tableau_merchant_analysis.csv")
merchant_export_count = merchant_export.count()
logger.info(f"Merchant analysis export: {merchant_export_count:,} rows")


# ══════════════════════════════════════════════
# DONE
# ══════════════════════════════════════════════
logger.info("═══ export_for_tableau job COMPLETE ═══")
logger.info(f"  Fraud overview:     {fraud_export_count:,} rows")
logger.info(f"  Customer risk:      {customer_export_count:,} rows")
logger.info(f"  Merchant analysis:  {merchant_export_count:,} rows")

job.commit()