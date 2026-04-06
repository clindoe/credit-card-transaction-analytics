"""
cloudwatch_metrics.py — Publishes custom CloudWatch metrics after pipeline runs.

Metrics published:
- BatchIngestionCount: records ingested per batch
- DataQualityScore: % of records passing all quality checks
- AnomalyDetectionCount: flagged transactions per run
- GlueJobDuration: time taken for each ETL job (seconds)

Usage:
    # After ingestion:
    python -m src.monitoring.cloudwatch_metrics --action post_ingestion

    # After full pipeline run:
    python -m src.monitoring.cloudwatch_metrics --action post_pipeline
"""

import boto3
import json
import argparse
import logging
from datetime import datetime, timezone

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
BUCKET = "credit-txn-analytics-adil"
REGION = "us-east-1"
NAMESPACE = "CreditTxnPipeline"   # Custom CloudWatch namespace

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

cloudwatch = boto3.client("cloudwatch", region_name=REGION)
s3_client = boto3.client("s3", region_name=REGION)
glue_client = boto3.client("glue", region_name=REGION)


def put_metric(metric_name: str, value: float, unit: str = "Count", dimensions: list = None):
    """Publish a single metric to CloudWatch."""
    metric_data = {
        "MetricName": metric_name,
        "Timestamp": datetime.now(timezone.utc),
        "Value": value,
        "Unit": unit,
    }
    if dimensions:
        metric_data["Dimensions"] = dimensions

    cloudwatch.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[metric_data]
    )
    logger.info(f"Published metric: {metric_name} = {value} {unit}")


def publish_ingestion_metrics():
    """
    Read the latest manifest file and publish ingestion metrics.
    """
    logger.info("Publishing ingestion metrics...")

    # Find the latest manifest
    response = s3_client.list_objects_v2(
        Bucket=BUCKET,
        Prefix="metadata/manifests/"
    )

    if "Contents" not in response:
        logger.warning("No manifest files found")
        return

    # Get the most recent manifest
    manifests = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
    latest_key = manifests[0]["Key"]

    # Download and parse
    obj = s3_client.get_object(Bucket=BUCKET, Key=latest_key)
    manifest = json.loads(obj["Body"].read().decode("utf-8"))

    # Publish metrics
    put_metric(
        "BatchIngestionCount",
        manifest.get("total_rows", 0),
        "Count",
        [{"Name": "Source", "Value": manifest.get("source_label", "unknown")}]
    )

    put_metric(
        "BatchCount",
        manifest.get("total_batches", 0),
        "Count"
    )

    put_metric(
        "FailedBatchCount",
        manifest.get("failed_batches", 0),
        "Count"
    )

    logger.info(f"Ingestion metrics published from manifest: {latest_key}")


def publish_quality_metrics():
    """
    Read the latest data quality report and publish quality metrics.
    """
    logger.info("Publishing data quality metrics...")

    response = s3_client.list_objects_v2(
        Bucket=BUCKET,
        Prefix="staging/data_quality_reports/"
    )

    if "Contents" not in response:
        logger.warning("No quality reports found")
        return

    # Get most recent report
    reports = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
    latest_key = reports[0]["Key"]

    obj = s3_client.get_object(Bucket=BUCKET, Key=latest_key)
    report = json.loads(obj["Body"].read().decode("utf-8"))

    quality_score = report.get("record_counts", {}).get("quality_score_pct", 0)
    put_metric("DataQualityScore", quality_score, "Percent")

    raw_count = report.get("record_counts", {}).get("raw_input", 0)
    final_count = report.get("record_counts", {}).get("final_output", 0)
    dropped = raw_count - final_count

    put_metric("RecordsDropped", dropped, "Count")
    put_metric("DuplicatesRemoved", report.get("record_counts", {}).get("duplicates_removed", 0), "Count")

    logger.info(f"Quality metrics published. Score: {quality_score}%")
    return quality_score


def publish_anomaly_metrics():
    """
    Read the analytics report and publish anomaly detection metrics.
    """
    logger.info("Publishing anomaly detection metrics...")

    response = s3_client.list_objects_v2(
        Bucket=BUCKET,
        Prefix="analytics/staging_to_analytics_report_"
    )

    if "Contents" not in response:
        logger.warning("No analytics reports found")
        return

    reports = sorted(response["Contents"], key=lambda x: x["LastModified"], reverse=True)
    latest_key = reports[0]["Key"]

    obj = s3_client.get_object(Bucket=BUCKET, Key=latest_key)
    report = json.loads(obj["Body"].read().decode("utf-8"))

    anomaly_stats = report.get("anomaly_detection_accuracy", {})

    put_metric(
        "AnomalyDetectionCount",
        anomaly_stats.get("total_anomalies_flagged", 0),
        "Count"
    )

    put_metric(
        "AnomalyTruePositives",
        anomaly_stats.get("true_positives", 0),
        "Count"
    )

    put_metric(
        "AnomalyPrecision",
        anomaly_stats.get("precision_pct", 0),
        "Percent"
    )

    put_metric(
        "AnomalyRecall",
        anomaly_stats.get("recall_pct", 0),
        "Percent"
    )

    logger.info(f"Anomaly metrics published. Flagged: {anomaly_stats.get('total_anomalies_flagged', 0)}")
    return anomaly_stats.get("total_anomalies_flagged", 0)


def publish_glue_job_metrics():
    """
    Query Glue for the most recent run of each job and publish duration metrics.
    """
    logger.info("Publishing Glue job duration metrics...")

    job_names = [
        "raw-to-staging",
        "staging-to-analytics",
        "fraud-feature-engineering",
        "export-for-tableau",
    ]

    for job_name in job_names:
        try:
            response = glue_client.get_job_runs(
                JobName=job_name,
                MaxResults=1
            )

            runs = response.get("JobRuns", [])
            if not runs:
                logger.warning(f"No runs found for {job_name}")
                continue

            latest_run = runs[0]
            duration = latest_run.get("ExecutionTime", 0)
            state = latest_run.get("JobRunState", "UNKNOWN")

            put_metric(
                "GlueJobDuration",
                duration,
                "Seconds",
                [{"Name": "JobName", "Value": job_name}]
            )

            # Publish job status as a metric (1 = succeeded, 0 = failed)
            put_metric(
                "GlueJobSuccess",
                1 if state == "SUCCEEDED" else 0,
                "Count",
                [{"Name": "JobName", "Value": job_name}]
            )

            logger.info(f"  {job_name}: {duration}s ({state})")

        except Exception as e:
            logger.error(f"Error fetching runs for {job_name}: {e}")


def publish_all_metrics():
    """Publish all metrics — call this after a full pipeline run."""
    logger.info("═══ Publishing all pipeline metrics ═══")

    publish_ingestion_metrics()
    quality_score = publish_quality_metrics()
    anomaly_count = publish_anomaly_metrics()
    publish_glue_job_metrics()

    logger.info("═══ All metrics published ═══")
    return quality_score, anomaly_count


# ──────────────────────────────────────────────
# CLI INTERFACE
# ──────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish pipeline metrics to CloudWatch")
    parser.add_argument(
        "--action",
        choices=["post_ingestion", "post_pipeline", "post_quality", "post_anomaly", "post_glue", "all"],
        default="all",
        help="Which metrics to publish"
    )
    args = parser.parse_args()

    if args.action == "post_ingestion":
        publish_ingestion_metrics()
    elif args.action == "post_quality":
        publish_quality_metrics()
    elif args.action == "post_anomaly":
        publish_anomaly_metrics()
    elif args.action == "post_glue":
        publish_glue_job_metrics()
    elif args.action in ("post_pipeline", "all"):
        publish_all_metrics()