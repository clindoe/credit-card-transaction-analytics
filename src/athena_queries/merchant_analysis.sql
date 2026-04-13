-- ================================================================
-- MERCHANT ANALYSIS QUERIES
-- Database: credit_txn_db
-- ================================================================


-- ----------------------------------------------------------------
-- QUERY 8: Repeat fraud merchant identification
-- Business question: Which merchants are repeatedly targeted by fraud?
-- ----------------------------------------------------------------
SELECT
    merchant,
    category,
    SUM(total_transactions) AS total_transactions,
    SUM(fraud_count) AS total_fraud_incidents,
    ROUND(SUM(fraud_count) * 100.0 / SUM(total_transactions), 4) AS fraud_rate_pct,
    ROUND(SUM(total_amount), 2) AS total_volume,
    SUM(unique_customers) AS total_unique_customers,
    COUNT(DISTINCT transaction_date) AS active_days,
    ROUND(AVG(avg_amount), 2) AS avg_transaction_amount
FROM credit_txn_db.analytics_daily_merchant_summary
GROUP BY merchant, category
HAVING SUM(fraud_count) >= 5
ORDER BY total_fraud_incidents DESC
LIMIT 50;


-- ----------------------------------------------------------------
-- QUERY 10: Category spending seasonality analysis
-- Business question: How does spending vary across months for each category?
-- ----------------------------------------------------------------
SELECT
    category,
    iso_year,
    iso_week,
    total_transactions,
    total_amount,
    avg_amount,
    wow_amount_change_pct,
    wow_txn_change_pct,
    -- Running average (4-week moving average)
    ROUND(AVG(total_amount) OVER (
        PARTITION BY category
        ORDER BY iso_year, iso_week
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ), 2) AS moving_avg_4wk_amount,
    -- Rank categories by weekly volume
    RANK() OVER (
        PARTITION BY iso_year, iso_week
        ORDER BY total_amount DESC
    ) AS volume_rank
FROM credit_txn_db.analytics_category_spending_trends
ORDER BY category, iso_year, iso_week;
