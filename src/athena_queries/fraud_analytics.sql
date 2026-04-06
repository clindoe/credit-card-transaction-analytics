-- ================================================================
-- FRAUD ANALYTICS QUERIES
-- Database: credit_txn_db
-- ================================================================


-- ----------------------------------------------------------------
-- QUERY 1: Fraud rate by merchant category
-- Business question: Which categories are most targeted by fraudsters?
-- ----------------------------------------------------------------
SELECT
    category,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END) AS legit_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_pct,
    ROUND(AVG(CASE WHEN is_fraud = 1 THEN amt ELSE NULL END), 2) AS avg_fraud_amount,
    ROUND(AVG(CASE WHEN is_fraud = 0 THEN amt ELSE NULL END), 2) AS avg_legit_amount,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN amt ELSE 0 END), 2) AS total_fraud_losses
FROM credit_txn_db.staging_transactions_cleaned
GROUP BY category
ORDER BY fraud_rate_pct DESC;


-- ----------------------------------------------------------------
-- QUERY 2: Average time between legitimate vs fraudulent transactions
-- Business question: Do fraudsters transact faster than normal customers?
-- Uses window functions to calculate inter-transaction time
-- ----------------------------------------------------------------
WITH txn_gaps AS (
    SELECT
        cc_num,
        is_fraud,
        unix_time,
        LAG(unix_time) OVER (PARTITION BY cc_num ORDER BY unix_time) AS prev_unix_time
    FROM credit_txn_db.staging_transactions_cleaned
)
SELECT
    CASE WHEN is_fraud = 1 THEN 'Fraudulent' ELSE 'Legitimate' END AS transaction_type,
    COUNT(*) AS transaction_count,
    ROUND(AVG(unix_time - prev_unix_time) / 3600, 2) AS avg_hours_between_txns,
    ROUND(MIN(unix_time - prev_unix_time) / 3600, 4) AS min_hours_between_txns,
    ROUND(APPROX_PERCENTILE(unix_time - prev_unix_time, 0.5) / 3600, 2) AS median_hours_between_txns
FROM txn_gaps
WHERE prev_unix_time IS NOT NULL
GROUP BY is_fraud
ORDER BY is_fraud;


-- ----------------------------------------------------------------
-- QUERY 3: Geographic regions with anomalous spending patterns
-- Business question: Where are fraud hotspots?
-- Uses geospatial binning by state
-- ----------------------------------------------------------------
SELECT
    state,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_pct,
    ROUND(AVG(amt), 2) AS avg_amount,
    ROUND(AVG(distance_from_home_km), 2) AS avg_distance_km,
    ROUND(AVG(CASE WHEN is_fraud = 1 THEN distance_from_home_km ELSE NULL END), 2) AS avg_fraud_distance_km,
    COUNT(DISTINCT cc_num) AS unique_customers
FROM credit_txn_db.staging_transactions_cleaned
GROUP BY state
HAVING COUNT(*) > 1000
ORDER BY fraud_rate_pct DESC
LIMIT 20;


-- ----------------------------------------------------------------
-- QUERY 4: False positive rate of rule-based anomaly detection
-- Business question: How accurate is our anomaly flagging?
-- Joins anomaly flags against actual fraud labels
-- ----------------------------------------------------------------
SELECT
    'Overall' AS detection_method,
    COUNT(*) AS total_flagged,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS true_positives,
    SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END) AS false_positives,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS precision_pct,
    ROUND(SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS false_positive_rate_pct
FROM credit_txn_db.analytics_fraud_detection_results

UNION ALL

SELECT
    'Amount Outlier Only' AS detection_method,
    COUNT(*) AS total_flagged,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS true_positives,
    SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END) AS false_positives,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS precision_pct,
    ROUND(SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS false_positive_rate_pct
FROM credit_txn_db.analytics_fraud_detection_results
WHERE flag_amount_outlier = true

UNION ALL

SELECT
    'Unusual Hour Only' AS detection_method,
    COUNT(*) AS total_flagged,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS true_positives,
    SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END) AS false_positives,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS precision_pct,
    ROUND(SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS false_positive_rate_pct
FROM credit_txn_db.analytics_fraud_detection_results
WHERE flag_unusual_hour = true

UNION ALL

SELECT
    'Geographic Outlier Only' AS detection_method,
    COUNT(*) AS total_flagged,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS true_positives,
    SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END) AS false_positives,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS precision_pct,
    ROUND(SUM(CASE WHEN is_fraud = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS false_positive_rate_pct
FROM credit_txn_db.analytics_fraud_detection_results
WHERE flag_geographic_outlier = true

ORDER BY detection_method;