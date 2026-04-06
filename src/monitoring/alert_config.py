"""
alert_config.py — SNS alert configuration and test utilities.

Stores alert thresholds and provides functions to:
- Check if metrics breach thresholds
- Send manual test alerts
- Document alert configuration
"""

import boto3
import json
import logging

# ──────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────
REGION = "us-east-1"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:471879937478:credit-txn-pipeline-alerts"
# ^^^ UPDATE THIS with your actual Topic ARN from Step 6a

ALERT_THRESHOLDS = {
    "data_quality_min_pct": 95.0,
    "anomaly_count_max": 250000,      # 2x normal baseline — update with your number
    "glue_job_max_duration_sec": 3600, # 1 hour
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

sns_client = boto3.client("sns", region_name=REGION)


def send_alert(subject: str, message: str):
    """Send an alert via SNS."""
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],  # SNS subject max 100 chars
            Message=message
        )
        logger.info(f"Alert sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")


def check_and_alert(quality_score: float = None, anomaly_count: int = None):
    """
    Check metrics against thresholds and send alerts if breached.
    Call this after publishing metrics.
    """
    alerts_sent = 0

    if quality_score is not None and quality_score < ALERT_THRESHOLDS["data_quality_min_pct"]:
        send_alert(
            "ALERT: Data Quality Below Threshold",
            f"Data quality score is {quality_score}%, "
            f"which is below the {ALERT_THRESHOLDS['data_quality_min_pct']}% threshold.\n\n"
            f"Action required: Check staging/data_quality_reports/ for details.\n"
            f"Possible causes: Schema changes in source data, data corruption, "
            f"upstream system issues."
        )
        alerts_sent += 1

    if anomaly_count is not None and anomaly_count > ALERT_THRESHOLDS["anomaly_count_max"]:
        send_alert(
            "ALERT: Anomaly Count Spike Detected",
            f"Anomaly count is {anomaly_count:,}, "
            f"which exceeds the {ALERT_THRESHOLDS['anomaly_count_max']:,} threshold (2x normal).\n\n"
            f"Action required: Review analytics/fraud_detection_results/ for new patterns.\n"
            f"Possible causes: New fraud campaign, threshold calibration needed, "
            f"data drift in transaction patterns."
        )
        alerts_sent += 1

    if alerts_sent == 0:
        logger.info("All metrics within normal thresholds")

    return alerts_sent


def send_test_alert():
    """Send a test alert to verify SNS is working."""
    send_alert(
        "TEST: Credit Txn Pipeline Alert System",
        "This is a test alert from the Credit Card Transaction Analytics Pipeline.\n\n"
        "If you received this, your SNS alerting is configured correctly.\n\n"
        f"Alert thresholds:\n"
        f"  - Data quality minimum: {ALERT_THRESHOLDS['data_quality_min_pct']}%\n"
        f"  - Anomaly count maximum: {ALERT_THRESHOLDS['anomaly_count_max']:,}\n"
        f"  - Glue job max duration: {ALERT_THRESHOLDS['glue_job_max_duration_sec']}s"
    )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Send a test alert")
    args = parser.parse_args()

    if args.test:
        send_test_alert()
    else:
        print("Use --test to send a test alert")
        print(f"Current thresholds: {json.dumps(ALERT_THRESHOLDS, indent=2)}")