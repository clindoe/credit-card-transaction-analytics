"""
raw_to_staging.py — AWS Glue PySpark Job
Medallion Architecture: Bronze (raw) → Silver (staging)

This job:
1. Reads raw Parquet from S3
2. Enforces schema (casts types, quarantines bad rows)
3. Handles nulls (flags and removes records with null critical fields)
4. Deduplicates (window function on trans_num)
5. Standardizes (merchant names, category values)
6. Enriches (transaction_hour, day_of_week, is_weekend, amount_bucket, distance_from_home)
7. Generates a data quality report
8. Writes clean data to staging/ and quality report to staging/data_quality_reports/
"""

import sys
import json
from datetime import datetime, timezone
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, LongType, TimestampType
)
import math
import boto3

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
BUCKET = "credit-txn-analytics-adil"
RAW_PATH = f"s3://{BUCKET}/raw/transactions/"
STAGING_PATH = f"s3://{BUCKET}/staging/transactions_cleaned/"
QUARANTINE_PATH = f"s3://{BUCKET}/staging/quarantine/"
QUALITY_REPORT_PATH = f"s3://{BUCKET}/staging/data_quality_reports/"

# ──────────────────────────────────────────────
# INITIALIZE GLUE CONTEXT
# ──────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()
logger.info("═══ Starting raw_to_staging job ═══")

# ──────────────────────────────────────────────
# STEP 1: READ RAW DATA
# ──────────────────────────────────────────────
logger.info(f"Reading raw Parquet from {RAW_PATH}")

df_raw = spark.read.parquet(RAW_PATH)
raw_count = df_raw.count()
logger.info(f"Raw record count: {raw_count}")

# ──────────────────────────────────────────────
# STEP 2: SCHEMA ENFORCEMENT
# ──────────────────────────────────────────────
logger.info("Enforcing schema...")

# Define expected schema with correct types
# The Kaggle dataset columns and their target types:
df_typed = df_raw

# Cast columns to expected types (strings stay as-is, numerics get cast)
cast_expressions = {
    "trans_date_trans_time": "timestamp",
    "cc_num": "long",
    "amt": "double",
    "lat": "double",
    "long": "double",
    "city_pop": "int",
    "unix_time": "long",
    "merch_lat": "double",
    "merch_long": "double",
    "is_fraud": "int",
    "zip": "string",
    "batch_id": "int",
}

for col_name, col_type in cast_expressions.items():
    if col_name in df_typed.columns:
        df_typed = df_typed.withColumn(col_name, F.col(col_name).cast(col_type))

# Identify rows that failed casting (nulls that weren't null before)
# A row is "bad" if amt or trans_date_trans_time became null after casting
# but was not null before
df_bad_rows = df_typed.filter(
    (F.col("amt").isNull()) | (F.col("trans_date_trans_time").isNull())
)
bad_row_count = df_bad_rows.count()
logger.info(f"Schema enforcement: {bad_row_count} rows failed casting")

# Quarantine bad rows
if bad_row_count > 0:
    df_bad_rows.write.mode("overwrite").parquet(QUARANTINE_PATH)
    logger.info(f"Quarantined {bad_row_count} rows to {QUARANTINE_PATH}")

# Keep only good rows
df_clean = df_typed.filter(
    F.col("amt").isNotNull() & F.col("trans_date_trans_time").isNotNull()
)

# ──────────────────────────────────────────────
# STEP 3: NULL HANDLING
# ──────────────────────────────────────────────
logger.info("Handling nulls...")

# Count nulls per column for the quality report
null_counts = {}
for col_name in df_clean.columns:
    null_count = df_clean.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        null_counts[col_name] = null_count

logger.info(f"Null counts: {null_counts}")

# Drop rows where critical fields are null
critical_columns = ["trans_num", "cc_num", "merchant", "category", "amt",
                     "trans_date_trans_time"]
before_null_drop = df_clean.count()
df_clean = df_clean.dropna(subset=critical_columns)
after_null_drop = df_clean.count()
null_dropped = before_null_drop - after_null_drop
logger.info(f"Dropped {null_dropped} rows with null critical fields")

# ──────────────────────────────────────────────
# STEP 4: DEDUPLICATION
# ──────────────────────────────────────────────
logger.info("Deduplicating...")

before_dedup = df_clean.count()

# Window: partition by trans_num, keep the latest record (by trans_date_trans_time)
window = Window.partitionBy("trans_num").orderBy(F.desc("trans_date_trans_time"))
df_clean = df_clean.withColumn("_row_num", F.row_number().over(window)) \
                    .filter(F.col("_row_num") == 1) \
                    .drop("_row_num")

after_dedup = df_clean.count()
duplicates_removed = before_dedup - after_dedup
logger.info(f"Removed {duplicates_removed} duplicate records")

# ──────────────────────────────────────────────
# STEP 5: STANDARDIZATION
# ──────────────────────────────────────────────
logger.info("Standardizing...")

# Normalize merchant names: trim whitespace, lowercase
df_clean = df_clean.withColumn("merchant", F.lower(F.trim(F.col("merchant"))))

# Standardize category values: trim, lowercase, replace underscores with spaces
df_clean = df_clean.withColumn("category", F.lower(F.trim(F.col("category"))))

# Standardize gender to uppercase
df_clean = df_clean.withColumn("gender", F.upper(F.trim(F.col("gender"))))

# Standardize state to uppercase
df_clean = df_clean.withColumn("state", F.upper(F.trim(F.col("state"))))

# ──────────────────────────────────────────────
# STEP 6: ENRICHMENT
# ──────────────────────────────────────────────
logger.info("Enriching with derived columns...")

# Extract time-based features
df_clean = df_clean.withColumn(
    "transaction_hour", F.hour(F.col("trans_date_trans_time"))
)
df_clean = df_clean.withColumn(
    "day_of_week", F.dayofweek(F.col("trans_date_trans_time"))
    # Spark: 1=Sunday, 2=Monday, ..., 7=Saturday
)
df_clean = df_clean.withColumn(
    "day_of_week_name", F.date_format(F.col("trans_date_trans_time"), "EEEE")
)
df_clean = df_clean.withColumn(
    "is_weekend",
    F.when(F.col("day_of_week").isin(1, 7), True).otherwise(False)
)

# Transaction date (date only, for partitioning)
df_clean = df_clean.withColumn(
    "transaction_date", F.to_date(F.col("trans_date_trans_time"))
)

# Amount buckets
df_clean = df_clean.withColumn(
    "amount_bucket",
    F.when(F.col("amt") < 10, "under_10")
     .when(F.col("amt") < 50, "10_to_50")
     .when(F.col("amt") < 200, "50_to_200")
     .when(F.col("amt") < 1000, "200_to_1000")
     .otherwise("over_1000")
)

# Haversine distance between customer location and merchant location
# Using Spark SQL functions instead of a UDF for better performance
df_clean = df_clean.withColumn("lat_rad", F.radians(F.col("lat")))
df_clean = df_clean.withColumn("long_rad", F.radians(F.col("long")))
df_clean = df_clean.withColumn("merch_lat_rad", F.radians(F.col("merch_lat")))
df_clean = df_clean.withColumn("merch_long_rad", F.radians(F.col("merch_long")))

df_clean = df_clean.withColumn(
    "dlat", F.col("merch_lat_rad") - F.col("lat_rad")
)
df_clean = df_clean.withColumn(
    "dlong", F.col("merch_long_rad") - F.col("long_rad")
)

# Haversine formula: a = sin²(Δlat/2) + cos(lat1) * cos(lat2) * sin²(Δlong/2)
df_clean = df_clean.withColumn(
    "a",
    F.pow(F.sin(F.col("dlat") / 2), 2) +
    F.cos(F.col("lat_rad")) * F.cos(F.col("merch_lat_rad")) *
    F.pow(F.sin(F.col("dlong") / 2), 2)
)

# distance = 2 * R * atan2(sqrt(a), sqrt(1-a))  [R = 6371 km]
df_clean = df_clean.withColumn(
    "distance_from_home_km",
    2 * 6371 * F.atan2(F.sqrt(F.col("a")), F.sqrt(1 - F.col("a")))
)

# Drop intermediate calculation columns
df_clean = df_clean.drop(
    "lat_rad", "long_rad", "merch_lat_rad", "merch_long_rad",
    "dlat", "dlong", "a"
)

# Round distance to 2 decimal places
df_clean = df_clean.withColumn(
    "distance_from_home_km",
    F.round(F.col("distance_from_home_km"), 2)
)

# ──────────────────────────────────────────────
# STEP 7: DROP UNNECESSARY COLUMNS
# ──────────────────────────────────────────────

# Drop the unnamed index column from the original CSV
if "Unnamed: 0" in df_clean.columns:
    df_clean = df_clean.drop("Unnamed: 0")
# Also check for the variant name
if "unnamed: 0" in [c.lower() for c in df_clean.columns]:
    for c in df_clean.columns:
        if c.lower() == "unnamed: 0":
            df_clean = df_clean.drop(c)

# ──────────────────────────────────────────────
# STEP 8: GENERATE DATA QUALITY REPORT
# ──────────────────────────────────────────────
logger.info("Generating data quality report...")

final_count = df_clean.count()

# Amount statistics
amt_stats = df_clean.agg(
    F.min("amt").alias("min_amount"),
    F.max("amt").alias("max_amount"),
    F.mean("amt").alias("mean_amount"),
    F.stddev("amt").alias("stddev_amount"),
    F.expr("percentile_approx(amt, 0.5)").alias("median_amount"),
).collect()[0]

# Fraud statistics
fraud_stats = df_clean.groupBy("is_fraud").count().collect()
fraud_dict = {str(row["is_fraud"]): row["count"] for row in fraud_stats}

# Category distribution
category_dist = df_clean.groupBy("category").count().orderBy(F.desc("count")).collect()
category_dict = {row["category"]: row["count"] for row in category_dist}

# Amount bucket distribution
bucket_dist = df_clean.groupBy("amount_bucket").count().orderBy("amount_bucket").collect()
bucket_dict = {row["amount_bucket"]: row["count"] for row in bucket_dist}

# Distance statistics
dist_stats = df_clean.agg(
    F.min("distance_from_home_km").alias("min_distance"),
    F.max("distance_from_home_km").alias("max_distance"),
    F.mean("distance_from_home_km").alias("mean_distance"),
).collect()[0]

quality_report = {
    "report_timestamp": datetime.now(timezone.utc).isoformat(),
    "job_name": args["JOB_NAME"],
    "record_counts": {
        "raw_input": raw_count,
        "schema_failures": bad_row_count,
        "null_dropped": null_dropped,
        "duplicates_removed": duplicates_removed,
        "final_output": final_count,
        "quality_score_pct": round((final_count / raw_count) * 100, 2) if raw_count > 0 else 0,
    },
    "null_counts_per_column": null_counts,
    "amount_statistics": {
        "min": float(amt_stats["min_amount"]) if amt_stats["min_amount"] else None,
        "max": float(amt_stats["max_amount"]) if amt_stats["max_amount"] else None,
        "mean": round(float(amt_stats["mean_amount"]), 2) if amt_stats["mean_amount"] else None,
        "stddev": round(float(amt_stats["stddev_amount"]), 2) if amt_stats["stddev_amount"] else None,
        "median": float(amt_stats["median_amount"]) if amt_stats["median_amount"] else None,
    },
    "fraud_distribution": fraud_dict,
    "category_distribution": category_dict,
    "amount_bucket_distribution": bucket_dict,
    "distance_statistics": {
        "min_km": float(dist_stats["min_distance"]) if dist_stats["min_distance"] else None,
        "max_km": float(dist_stats["max_distance"]) if dist_stats["max_distance"] else None,
        "mean_km": round(float(dist_stats["mean_distance"]), 2) if dist_stats["mean_distance"] else None,
    },
}

# Write quality report as JSON to S3
report_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
report_key = f"staging/data_quality_reports/quality_report_{report_timestamp}.json"
report_json = json.dumps(quality_report, indent=2)

s3_client = boto3.client("s3")
s3_client.put_object(
    Bucket=BUCKET,
    Key=report_key,
    Body=report_json.encode("utf-8"),
    ContentType="application/json"
)
logger.info(f"Quality report written to s3://{BUCKET}/{report_key}")

# ──────────────────────────────────────────────
# STEP 9: WRITE CLEAN DATA TO STAGING
# ──────────────────────────────────────────────
logger.info(f"Writing {final_count} clean records to {STAGING_PATH}")

# Write as Parquet, partitioned by year and month for efficient querying
df_clean.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(STAGING_PATH)

logger.info("═══ raw_to_staging job COMPLETE ═══")
logger.info(f"  Input:  {raw_count:,} records")
logger.info(f"  Output: {final_count:,} records")
logger.info(f"  Quality score: {quality_report['record_counts']['quality_score_pct']}%")

# ──────────────────────────────────────────────
# COMMIT JOB
# ──────────────────────────────────────────────
job.commit()