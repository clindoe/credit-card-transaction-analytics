"""
fraud_feature_engineering.py — AWS Glue PySpark Job
Creates ML-ready feature table from staging data.

Features:
- Lag features: previous transaction amount, time since last transaction
- Velocity features: transaction count in last 1hr, 6hr, 24hr
- Geographic features: distance from last transaction, distance from home
- Customer behavioral stats joined in
"""

import sys
import json
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import boto3

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
BUCKET = "credit-txn-analytics-adil"
STAGING_PATH = f"s3://{BUCKET}/staging/transactions_cleaned/"
CUSTOMER_RISK_PATH = f"s3://{BUCKET}/analytics/customer_risk_scores/"
FEATURE_TABLE_PATH = f"s3://{BUCKET}/analytics/fraud_feature_table/"

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
logger.info("═══ Starting fraud_feature_engineering job ═══")

# ──────────────────────────────────────────────
# READ DATA
# ──────────────────────────────────────────────
df = spark.read.parquet(STAGING_PATH)
logger.info(f"Loaded {df.count():,} records from staging")

# Ensure unix_time is available for time-based calculations
# If not present, derive from trans_date_trans_time
if "unix_time" not in df.columns:
    df = df.withColumn("unix_time", F.unix_timestamp(F.col("trans_date_trans_time")))

# ══════════════════════════════════════════════
# LAG FEATURES
# ══════════════════════════════════════════════
logger.info("Computing lag features...")

# Window: per customer, ordered by timestamp
customer_time_window = Window.partitionBy("cc_num").orderBy("unix_time")

# Previous transaction amount
df = df.withColumn(
    "prev_txn_amount",
    F.lag("amt", 1).over(customer_time_window)
)

# Amount change from previous transaction
df = df.withColumn(
    "amt_change_from_prev",
    F.when(F.col("prev_txn_amount").isNotNull(),
           F.col("amt") - F.col("prev_txn_amount"))
     .otherwise(None)
)

# Ratio to previous amount
df = df.withColumn(
    "amt_ratio_to_prev",
    F.when(
        F.col("prev_txn_amount").isNotNull() & (F.col("prev_txn_amount") > 0),
        F.round(F.col("amt") / F.col("prev_txn_amount"), 4)
    ).otherwise(None)
)

# Time since last transaction (seconds)
df = df.withColumn(
    "prev_txn_time",
    F.lag("unix_time", 1).over(customer_time_window)
)
df = df.withColumn(
    "time_since_last_txn_seconds",
    F.when(F.col("prev_txn_time").isNotNull(),
           F.col("unix_time") - F.col("prev_txn_time"))
     .otherwise(None)
)

# Time since last transaction (hours, more readable)
df = df.withColumn(
    "time_since_last_txn_hours",
    F.when(F.col("time_since_last_txn_seconds").isNotNull(),
           F.round(F.col("time_since_last_txn_seconds") / 3600, 2))
     .otherwise(None)
)

# ══════════════════════════════════════════════
# VELOCITY FEATURES
# ══════════════════════════════════════════════
logger.info("Computing velocity features...")

# Velocity: count of transactions within time windows
# We use range-based windows on unix_time

# 1-hour window (3600 seconds before current transaction)
one_hour_window = (
    Window.partitionBy("cc_num")
    .orderBy("unix_time")
    .rangeBetween(-3600, 0)
)

# 6-hour window
six_hour_window = (
    Window.partitionBy("cc_num")
    .orderBy("unix_time")
    .rangeBetween(-21600, 0)
)

# 24-hour window
twenty_four_hour_window = (
    Window.partitionBy("cc_num")
    .orderBy("unix_time")
    .rangeBetween(-86400, 0)
)

df = df.withColumn(
    "txn_count_1hr",
    F.count("*").over(one_hour_window) - 1  # subtract self
)
df = df.withColumn(
    "txn_count_6hr",
    F.count("*").over(six_hour_window) - 1
)
df = df.withColumn(
    "txn_count_24hr",
    F.count("*").over(twenty_four_hour_window) - 1
)

# Amount velocity — total spent in window
df = df.withColumn(
    "amt_sum_1hr",
    F.round(F.sum("amt").over(one_hour_window) - F.col("amt"), 2)
)
df = df.withColumn(
    "amt_sum_24hr",
    F.round(F.sum("amt").over(twenty_four_hour_window) - F.col("amt"), 2)
)

# ══════════════════════════════════════════════
# GEOGRAPHIC FEATURES
# ══════════════════════════════════════════════
logger.info("Computing geographic features...")

# Previous transaction location
df = df.withColumn(
    "prev_merch_lat",
    F.lag("merch_lat", 1).over(customer_time_window)
)
df = df.withColumn(
    "prev_merch_long",
    F.lag("merch_long", 1).over(customer_time_window)
)

# Haversine distance from previous transaction location
df = df.withColumn("_lat1", F.radians(F.col("prev_merch_lat")))
df = df.withColumn("_lon1", F.radians(F.col("prev_merch_long")))
df = df.withColumn("_lat2", F.radians(F.col("merch_lat")))
df = df.withColumn("_lon2", F.radians(F.col("merch_long")))
df = df.withColumn("_dlat", F.col("_lat2") - F.col("_lat1"))
df = df.withColumn("_dlon", F.col("_lon2") - F.col("_lon1"))
df = df.withColumn(
    "_a",
    F.pow(F.sin(F.col("_dlat") / 2), 2) +
    F.cos(F.col("_lat1")) * F.cos(F.col("_lat2")) *
    F.pow(F.sin(F.col("_dlon") / 2), 2)
)
df = df.withColumn(
    "distance_from_last_txn_km",
    F.when(F.col("prev_merch_lat").isNotNull(),
           F.round(2 * 6371 * F.atan2(F.sqrt(F.col("_a")), F.sqrt(1 - F.col("_a"))), 2))
     .otherwise(None)
)

# Speed between last two transactions (km/h)
df = df.withColumn(
    "speed_from_last_txn_kmh",
    F.when(
        F.col("distance_from_last_txn_km").isNotNull() &
        F.col("time_since_last_txn_hours").isNotNull() &
        (F.col("time_since_last_txn_hours") > 0),
        F.round(F.col("distance_from_last_txn_km") / F.col("time_since_last_txn_hours"), 2)
    ).otherwise(None)
)

# Impossible travel flag (speed > 900 km/h ≈ airplane speed)
df = df.withColumn(
    "flag_impossible_travel",
    F.when(F.col("speed_from_last_txn_kmh") > 900, True).otherwise(False)
)

# Drop intermediate columns
df = df.drop(
    "_lat1", "_lon1", "_lat2", "_lon2", "_dlat", "_dlon", "_a",
    "prev_merch_lat", "prev_merch_long", "prev_txn_time"
)

# ══════════════════════════════════════════════
# JOIN CUSTOMER RISK SCORES
# ══════════════════════════════════════════════
logger.info("Joining customer risk scores...")

try:
    customer_risk = spark.read.parquet(CUSTOMER_RISK_PATH)
    # Select only the score columns to avoid duplicate columns after join
    risk_cols = customer_risk.select(
        "cc_num", "risk_score", "risk_tier",
        "avg_transaction_amount", "fraud_count"
    ).withColumnRenamed("fraud_count", "customer_total_fraud_count") \
     .withColumnRenamed("avg_transaction_amount", "customer_avg_amount")

    df = df.join(risk_cols, on="cc_num", how="left")
    logger.info("Customer risk scores joined successfully")
except Exception as e:
    logger.warning(f"Could not join customer risk scores: {e}")
    # Add placeholder columns so schema is consistent
    df = df.withColumn("risk_score", F.lit(None).cast("int"))
    df = df.withColumn("risk_tier", F.lit(None).cast("string"))
    df = df.withColumn("customer_avg_amount", F.lit(None).cast("double"))
    df = df.withColumn("customer_total_fraud_count", F.lit(None).cast("int"))

# ══════════════════════════════════════════════
# WRITE OUTPUT
# ══════════════════════════════════════════════
final_count = df.count()
logger.info(f"Writing {final_count:,} records to {FEATURE_TABLE_PATH}")

df.write \
    .mode("overwrite") \
    .parquet(FEATURE_TABLE_PATH)

logger.info("═══ fraud_feature_engineering job COMPLETE ═══")
logger.info(f"  Output records: {final_count:,}")
logger.info(f"  Total columns: {len(df.columns)}")

job.commit()