-- ================================================================
-- CUSTOMER BEHAVIOR QUERIES
-- Database: credit_txn_db
-- ================================================================


-- ----------------------------------------------------------------
-- QUERY 5: Customer segmentation by spending behavior
-- Business question: How can we group customers by their spending patterns?
-- ----------------------------------------------------------------
SELECT
    CASE
        WHEN avg_transaction_amount >= 200 AND total_transactions >= 100 THEN 'High Value / High Frequency'
        WHEN avg_transaction_amount >= 200 AND total_transactions < 100 THEN 'High Value / Low Frequency'
        WHEN avg_transaction_amount < 200 AND total_transactions >= 100 THEN 'Low Value / High Frequency'
        ELSE 'Low Value / Low Frequency'
    END AS customer_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_spend), 2) AS avg_total_spend,
    ROUND(AVG(avg_transaction_amount), 2) AS avg_txn_amount,
    ROUND(AVG(total_transactions), 0) AS avg_txn_count,
    ROUND(AVG(fraud_count), 2) AS avg_fraud_incidents,
    ROUND(AVG(risk_score), 2) AS avg_risk_score,
    ROUND(AVG(unique_merchants), 0) AS avg_unique_merchants
FROM credit_txn_db.analytics_customer_risk_scores
GROUP BY 1
ORDER BY avg_total_spend DESC;


-- ----------------------------------------------------------------
-- QUERY 6: Peak fraud hours analysis
-- Business question: When are fraudulent transactions most likely to occur?
-- ----------------------------------------------------------------
SELECT
    transaction_hour,
    CASE
        WHEN transaction_hour BETWEEN 0 AND 5 THEN 'Late Night'
        WHEN transaction_hour BETWEEN 6 AND 11 THEN 'Morning'
        WHEN transaction_hour BETWEEN 12 AND 17 THEN 'Afternoon'
        ELSE 'Evening'
    END AS time_period,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_pct,
    ROUND(AVG(amt), 2) AS avg_amount,
    ROUND(AVG(CASE WHEN is_fraud = 1 THEN amt ELSE NULL END), 2) AS avg_fraud_amount
FROM credit_txn_db.staging_transactions_cleaned
GROUP BY transaction_hour
ORDER BY transaction_hour;


-- ----------------------------------------------------------------
-- QUERY 7: High-value transaction patterns
-- Business question: Are high-value transactions more likely to be fraud?
-- ----------------------------------------------------------------
SELECT
    amount_bucket,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_pct,
    ROUND(AVG(amt), 2) AS avg_amount,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN amt ELSE 0 END), 2) AS total_fraud_losses,
    ROUND(AVG(distance_from_home_km), 2) AS avg_distance,
    COUNT(DISTINCT cc_num) AS unique_customers
FROM credit_txn_db.staging_transactions_cleaned
GROUP BY amount_bucket
ORDER BY
    CASE amount_bucket
        WHEN 'under_10' THEN 1
        WHEN '10_to_50' THEN 2
        WHEN '50_to_200' THEN 3
        WHEN '200_to_1000' THEN 4
        WHEN 'over_1000' THEN 5
    END;


-- ----------------------------------------------------------------
-- QUERY 9: Customer lifetime fraud risk trajectory
-- Business question: How does fraud risk evolve over a customer's lifetime?
-- Groups customers by account age and compares risk scores
-- ----------------------------------------------------------------
SELECT
    CASE
        WHEN active_days <= 30 THEN '0-30 days'
        WHEN active_days <= 90 THEN '31-90 days'
        WHEN active_days <= 180 THEN '91-180 days'
        WHEN active_days <= 365 THEN '181-365 days'
        ELSE '365+ days'
    END AS account_age_bucket,
    COUNT(*) AS customer_count,
    ROUND(AVG(risk_score), 2) AS avg_risk_score,
    ROUND(AVG(fraud_rate), 4) AS avg_fraud_rate,
    ROUND(AVG(total_transactions), 0) AS avg_txn_count,
    ROUND(AVG(total_spend), 2) AS avg_total_spend,
    SUM(CASE WHEN risk_tier = 'Critical' THEN 1 ELSE 0 END) AS critical_risk_count,
    SUM(CASE WHEN risk_tier = 'High' THEN 1 ELSE 0 END) AS high_risk_count
FROM credit_txn_db.analytics_customer_risk_scores
GROUP BY 1
ORDER BY
    CASE
        WHEN active_days <= 30 THEN 1
        WHEN active_days <= 90 THEN 2
        WHEN active_days <= 180 THEN 3
        WHEN active_days <= 365 THEN 4
        ELSE 5
    END;