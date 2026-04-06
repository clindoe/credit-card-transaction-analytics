"""
staging_to_analytics.py — AWS Glue PySpark Job
Medallion Architecture: Silver (staging) → Gold (analytics)

Produces four analytics tables:
1. daily_merchant_summary — aggregated merchant performance + fraud metrics
2. customer_risk_scores — per-customer risk scoring with composite score
3. category_spending_trends — weekly category aggregations + WoW change
4. anomaly_flags — transactions flagged by statistical/behavioral rules
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
from pyspark.sql.types import DoubleType
import boto3

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
BUCKET = "credit-txn-analytics-adil"
STAGING_PATH = f"s3://{BUCKET}/staging/transactions_cleaned/"
ANALYTICS_BASE = f"s3://{BUCKET}/analytics"
MERCHANT_SUMMARY_PATH = f"{ANALYTICS_BASE}/daily_merchant_summary/"
CUSTOMER_RISK_PATH = f"{ANALYTICS_BASE}/customer_risk_scores/"
CATEGORY_TRENDS_PATH = f"{ANALYTICS_BASE}/category_spending_trends/"
ANOMALY_FLAGS_PATH = f"{ANALYTICS_BASE}/fraud_detection_results/"

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
logger.info("═══ Starting staging_to_analytics job ═══")

# ──────────────────────────────────────────────
# READ STAGING DATA
# ──────────────────────────────────────────────
logger.info(f"Reading staging data from {STAGING_PATH}")
df = spark.read.parquet(STAGING_PATH)
total_records = df.count()
logger.info(f"Loaded {total_records:,} records from staging")

# Cache the DataFrame since we'll read it multiple times
df.cache()

# Ensure transaction_date exists as a date column
if "transaction_date" not in df.columns:
    df = df.withColumn("transaction_date", F.to_date(F.col("trans_date_trans_time")))


# ══════════════════════════════════════════════
# TABLE 1: DAILY MERCHANT SUMMARY
# ══════════════════════════════════════════════
logger.info("Building daily_merchant_summary...")

# Extract year and month for partitioning
df_with_parts = df.withColumn("summary_year", F.year(F.col("transaction_date"))) \
                  .withColumn("summary_month", F.month(F.col("transaction_date")))

merchant_summary = df_with_parts.groupBy(
    "merchant", "category", "transaction_date", "summary_year", "summary_month"
).agg(
    F.count("*").alias("total_transactions"),
    F.sum("amt").alias("total_amount"),
    F.round(F.avg("amt"), 2).alias("avg_amount"),
    F.min("amt").alias("min_amount"),
    F.max("amt").alias("max_amount"),
    F.round(F.stddev("amt"), 2).alias("stddev_amount"),
    F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count"),
    F.sum(F.when(F.col("is_fraud") == 0, 1).otherwise(0)).alias("legit_count"),
    F.countDistinct("cc_num").alias("unique_customers"),
    F.round(F.avg("distance_from_home_km"), 2).alias("avg_distance_km"),
)

# Add fraud rate
merchant_summary = merchant_summary.withColumn(
    "fraud_rate",
    F.round(
        F.when(F.col("total_transactions") > 0,
               F.col("fraud_count") / F.col("total_transactions") * 100)
         .otherwise(0), 4
    )
)

# Write partitioned by year/month
merchant_summary.write \
    .mode("overwrite") \
    .partitionBy("summary_year", "summary_month") \
    .parquet(MERCHANT_SUMMARY_PATH)

merchant_count = merchant_summary.count()
logger.info(f"daily_merchant_summary: {merchant_count:,} rows written")


# ══════════════════════════════════════════════
# TABLE 2: CUSTOMER RISK SCORES
# ══════════════════════════════════════════════
logger.info("Building customer_risk_scores...")

customer_agg = df.groupBy("cc_num", "first", "last", "gender", "state", "city", "dob").agg(
    # Transaction behavior
    F.count("*").alias("total_transactions"),
    F.round(F.avg("amt"), 2).alias("avg_transaction_amount"),
    F.round(F.stddev("amt"), 2).alias("stddev_transaction_amount"),
    F.max("amt").alias("max_single_transaction"),
    F.min("amt").alias("min_single_transaction"),
    F.round(F.sum("amt"), 2).alias("total_spend"),

    # Merchant diversity
    F.countDistinct("merchant").alias("unique_merchants"),
    F.countDistinct("category").alias("unique_categories"),

    # Time patterns
    F.min("trans_date_trans_time").alias("first_transaction"),
    F.max("trans_date_trans_time").alias("last_transaction"),
    F.countDistinct("transaction_date").alias("active_days"),

    # Fraud history
    F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count"),

    # Weekend behavior
    F.sum(F.when(F.col("is_weekend") == True, 1).otherwise(0)).alias("weekend_transactions"),

    # Late night transactions (2 AM - 5 AM)
    F.sum(
        F.when((F.col("transaction_hour") >= 2) & (F.col("transaction_hour") <= 5), 1)
         .otherwise(0)
    ).alias("late_night_transactions"),

    # Distance
    F.round(F.avg("distance_from_home_km"), 2).alias("avg_distance_km"),
    F.round(F.max("distance_from_home_km"), 2).alias("max_distance_km"),
)

# Calculate derived risk features
customer_agg = customer_agg.withColumn(
    "fraud_rate",
    F.round(
        F.when(F.col("total_transactions") > 0,
               F.col("fraud_count") / F.col("total_transactions") * 100)
         .otherwise(0), 4
    )
)

customer_agg = customer_agg.withColumn(
    "weekend_ratio",
    F.round(
        F.when(F.col("total_transactions") > 0,
               F.col("weekend_transactions") / F.col("total_transactions"))
         .otherwise(0), 4
    )
)

customer_agg = customer_agg.withColumn(
    "late_night_ratio",
    F.round(
        F.when(F.col("total_transactions") > 0,
               F.col("late_night_transactions") / F.col("total_transactions"))
         .otherwise(0), 4
    )
)

# Transaction frequency (avg transactions per active day)
customer_agg = customer_agg.withColumn(
    "txn_frequency",
    F.round(
        F.when(F.col("active_days") > 0,
               F.col("total_transactions") / F.col("active_days"))
         .otherwise(0), 2
    )
)

# ── COMPOSITE RISK SCORE ──
# Rule-based scoring (0-100 scale):
#   - Fraud history:       40 points max (heaviest weight)
#   - High avg amount:     15 points max
#   - High max transaction:10 points max
#   - Late night ratio:    15 points max
#   - High distance:       10 points max
#   - Low merchant diversity (relative to txn count): 10 points max

# Fraud history component (0-40)
customer_agg = customer_agg.withColumn(
    "risk_fraud_history",
    F.least(
        F.lit(40),
        F.col("fraud_count") * 10  # Each fraud event = 10 points, capped at 40
    )
)

# High amount component (0-15) — based on percentile thresholds
customer_agg = customer_agg.withColumn(
    "risk_high_amount",
    F.when(F.col("avg_transaction_amount") > 500, 15)
     .when(F.col("avg_transaction_amount") > 200, 10)
     .when(F.col("avg_transaction_amount") > 100, 5)
     .otherwise(0)
)

# Max transaction component (0-10)
customer_agg = customer_agg.withColumn(
    "risk_max_txn",
    F.when(F.col("max_single_transaction") > 1000, 10)
     .when(F.col("max_single_transaction") > 500, 6)
     .when(F.col("max_single_transaction") > 200, 3)
     .otherwise(0)
)

# Late night component (0-15)
customer_agg = customer_agg.withColumn(
    "risk_late_night",
    F.when(F.col("late_night_ratio") > 0.2, 15)
     .when(F.col("late_night_ratio") > 0.1, 10)
     .when(F.col("late_night_ratio") > 0.05, 5)
     .otherwise(0)
)

# Distance component (0-10)
customer_agg = customer_agg.withColumn(
    "risk_distance",
    F.when(F.col("avg_distance_km") > 100, 10)
     .when(F.col("avg_distance_km") > 50, 6)
     .when(F.col("avg_distance_km") > 20, 3)
     .otherwise(0)
)

# Low diversity component (0-10)
customer_agg = customer_agg.withColumn(
    "risk_low_diversity",
    F.when(
        (F.col("total_transactions") > 20) & (F.col("unique_merchants") < 3), 10
    ).when(
        (F.col("total_transactions") > 20) & (F.col("unique_merchants") < 5), 5
    ).otherwise(0)
)

# Final composite risk score
customer_agg = customer_agg.withColumn(
    "risk_score",
    F.col("risk_fraud_history") + F.col("risk_high_amount") +
    F.col("risk_max_txn") + F.col("risk_late_night") +
    F.col("risk_distance") + F.col("risk_low_diversity")
)

# Risk tier
customer_agg = customer_agg.withColumn(
    "risk_tier",
    F.when(F.col("risk_score") >= 60, "Critical")
     .when(F.col("risk_score") >= 30, "High")
     .when(F.col("risk_score") >= 10, "Medium")
     .otherwise("Low")
)

customer_agg.write \
    .mode("overwrite") \
    .parquet(CUSTOMER_RISK_PATH)

customer_count = customer_agg.count()
logger.info(f"customer_risk_scores: {customer_count:,} rows written")


# ══════════════════════════════════════════════
# TABLE 3: CATEGORY SPENDING TRENDS
# ══════════════════════════════════════════════
logger.info("Building category_spending_trends...")

# Add ISO week number for weekly aggregation
df_weekly = df.withColumn(
    "iso_year", F.year(F.col("transaction_date"))
).withColumn(
    "iso_week", F.weekofyear(F.col("transaction_date"))
).withColumn(
    "week_start", F.date_sub(
        F.col("transaction_date"),
        F.dayofweek(F.col("transaction_date")) - 1
    )
)

category_trends = df_weekly.groupBy(
    "category", "iso_year", "iso_week", "week_start"
).agg(
    F.count("*").alias("total_transactions"),
    F.round(F.sum("amt"), 2).alias("total_amount"),
    F.round(F.avg("amt"), 2).alias("avg_amount"),
    F.countDistinct("cc_num").alias("unique_customers"),
    F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count"),
    F.round(F.avg("distance_from_home_km"), 2).alias("avg_distance_km"),
    F.sum(F.when(F.col("is_weekend") == True, 1).otherwise(0)).alias("weekend_count"),
)

# Add fraud rate
category_trends = category_trends.withColumn(
    "fraud_rate",
    F.round(
        F.when(F.col("total_transactions") > 0,
               F.col("fraud_count") / F.col("total_transactions") * 100)
         .otherwise(0), 4
    )
)

# Week-over-week change using window functions
wow_window = Window.partitionBy("category").orderBy("iso_year", "iso_week")

category_trends = category_trends.withColumn(
    "prev_week_amount",
    F.lag("total_amount", 1).over(wow_window)
)
category_trends = category_trends.withColumn(
    "prev_week_transactions",
    F.lag("total_transactions", 1).over(wow_window)
)

# WoW % change in total amount
category_trends = category_trends.withColumn(
    "wow_amount_change_pct",
    F.round(
        F.when(
            F.col("prev_week_amount").isNotNull() & (F.col("prev_week_amount") > 0),
            ((F.col("total_amount") - F.col("prev_week_amount")) / F.col("prev_week_amount")) * 100
        ).otherwise(None), 2
    )
)

# WoW % change in transaction count
category_trends = category_trends.withColumn(
    "wow_txn_change_pct",
    F.round(
        F.when(
            F.col("prev_week_transactions").isNotNull() & (F.col("prev_week_transactions") > 0),
            ((F.col("total_transactions") - F.col("prev_week_transactions"))
             / F.col("prev_week_transactions")) * 100
        ).otherwise(None), 2
    )
)

category_trends.write \
    .mode("overwrite") \
    .partitionBy("iso_year") \
    .parquet(CATEGORY_TRENDS_PATH)

trend_count = category_trends.count()
logger.info(f"category_spending_trends: {trend_count:,} rows written")


# ══════════════════════════════════════════════
# TABLE 4: ANOMALY FLAGS
# ══════════════════════════════════════════════
logger.info("Building anomaly_flags...")

# Calculate per-customer statistics for anomaly detection
customer_stats = df.groupBy("cc_num").agg(
    F.avg("amt").alias("cust_avg_amt"),
    F.stddev("amt").alias("cust_stddev_amt"),
    F.avg("transaction_hour").alias("cust_avg_hour"),
    F.stddev("transaction_hour").alias("cust_stddev_hour"),
    F.avg("distance_from_home_km").alias("cust_avg_distance"),
    F.stddev("distance_from_home_km").alias("cust_stddev_distance"),
)

# Handle nulls in stddev (customers with only 1 transaction)
customer_stats = customer_stats.fillna({
    "cust_stddev_amt": 0,
    "cust_stddev_hour": 0,
    "cust_stddev_distance": 0,
})

# Join customer stats back to transactions
df_with_stats = df.join(customer_stats, on="cc_num", how="left")

# ── FLAG 1: Amount outlier (> 3σ from customer mean) ──
df_with_stats = df_with_stats.withColumn(
    "flag_amount_outlier",
    F.when(
        (F.col("cust_stddev_amt") > 0) &
        (F.abs(F.col("amt") - F.col("cust_avg_amt")) > 3 * F.col("cust_stddev_amt")),
        True
    ).otherwise(False)
)

# ── FLAG 2: Unusual hour (2 AM - 5 AM) ──
df_with_stats = df_with_stats.withColumn(
    "flag_unusual_hour",
    F.when(
        (F.col("transaction_hour") >= 2) & (F.col("transaction_hour") <= 5),
        True
    ).otherwise(False)
)

# ── FLAG 3: Geographic outlier (> 3σ from customer's usual distance) ──
df_with_stats = df_with_stats.withColumn(
    "flag_geographic_outlier",
    F.when(
        (F.col("cust_stddev_distance") > 0) &
        (F.col("distance_from_home_km") - F.col("cust_avg_distance") >
         3 * F.col("cust_stddev_distance")),
        True
    ).otherwise(False)
)

# ── FLAG 4: High-value transaction (> $500) ──
df_with_stats = df_with_stats.withColumn(
    "flag_high_value",
    F.when(F.col("amt") > 500, True).otherwise(False)
)

# ── Combine: any flag triggered ──
df_with_stats = df_with_stats.withColumn(
    "is_anomaly",
    F.col("flag_amount_outlier") | F.col("flag_unusual_hour") |
    F.col("flag_geographic_outlier") | F.col("flag_high_value")
)

# Build reason tags
df_with_stats = df_with_stats.withColumn(
    "anomaly_reasons",
    F.concat_ws(", ",
        F.when(F.col("flag_amount_outlier"), F.lit("amount_outlier")),
        F.when(F.col("flag_unusual_hour"), F.lit("unusual_hour")),
        F.when(F.col("flag_geographic_outlier"), F.lit("geographic_outlier")),
        F.when(F.col("flag_high_value"), F.lit("high_value")),
    )
)

# Filter to only flagged transactions
anomalies = df_with_stats.filter(F.col("is_anomaly") == True)

# Select relevant columns for the anomaly table
anomaly_output = anomalies.select(
    "trans_num", "trans_date_trans_time", "cc_num",
    "first", "last", "merchant", "category",
    "amt", "state", "city",
    "lat", "long", "merch_lat", "merch_long",
    "is_fraud", "transaction_hour", "day_of_week_name",
    "is_weekend", "distance_from_home_km",
    "cust_avg_amt", "cust_stddev_amt",
    "cust_avg_distance", "cust_stddev_distance",
    "flag_amount_outlier", "flag_unusual_hour",
    "flag_geographic_outlier", "flag_high_value",
    "is_anomaly", "anomaly_reasons",
)

# Add year/month for partitioning
anomaly_output = anomaly_output.withColumn(
    "anomaly_year", F.year(F.col("trans_date_trans_time"))
).withColumn(
    "anomaly_month", F.month(F.col("trans_date_trans_time"))
)

anomaly_output.write \
    .mode("overwrite") \
    .partitionBy("anomaly_year", "anomaly_month") \
    .parquet(ANOMALY_FLAGS_PATH)

anomaly_count = anomaly_output.count()
logger.info(f"anomaly_flags: {anomaly_count:,} rows written")

# ── Anomaly accuracy stats (for the quality report) ──
true_positives = anomalies.filter(F.col("is_fraud") == 1).count()
false_positives = anomalies.filter(F.col("is_fraud") == 0).count()
total_fraud = df.filter(F.col("is_fraud") == 1).count()
false_negatives = total_fraud - true_positives

# ══════════════════════════════════════════════
# SUMMARY REPORT
# ══════════════════════════════════════════════
summary = {
    "report_timestamp": datetime.now(timezone.utc).isoformat(),
    "job_name": args["JOB_NAME"],
    "input_records": total_records,
    "outputs": {
        "daily_merchant_summary": merchant_count,
        "customer_risk_scores": customer_count,
        "category_spending_trends": trend_count,
        "anomaly_flags": anomaly_count,
    },
    "anomaly_detection_accuracy": {
        "total_anomalies_flagged": anomaly_count,
        "true_positives": true_positives,
        "false_positives": false_positives,
        "false_negatives": false_negatives,
        "precision_pct": round(true_positives / anomaly_count * 100, 2) if anomaly_count > 0 else 0,
        "recall_pct": round(true_positives / total_fraud * 100, 2) if total_fraud > 0 else 0,
    }
}

s3_client = boto3.client("s3")
report_key = f"analytics/staging_to_analytics_report_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
s3_client.put_object(
    Bucket=BUCKET,
    Key=report_key,
    Body=json.dumps(summary, indent=2).encode("utf-8"),
    ContentType="application/json"
)

logger.info("═══ staging_to_analytics job COMPLETE ═══")
logger.info(f"  Merchant summary: {merchant_count:,} rows")
logger.info(f"  Customer risk:    {customer_count:,} rows")
logger.info(f"  Category trends:  {trend_count:,} rows")
logger.info(f"  Anomaly flags:    {anomaly_count:,} rows")
logger.info(f"  Anomaly precision: {summary['anomaly_detection_accuracy']['precision_pct']}%")
logger.info(f"  Anomaly recall:    {summary['anomaly_detection_accuracy']['recall_pct']}%")

# Uncache
df.unpersist()

job.commit()