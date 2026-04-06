-- ================================================================
-- DATA QUALITY CHECK QUERIES
-- Database: credit_txn_db
-- Use these to validate pipeline output after each run
-- ================================================================


-- Row count comparison: raw vs staging
-- Expected: staging should be close to raw (minus dropped/deduped rows)
SELECT
    'raw' AS layer,
    COUNT(*) AS row_count
FROM credit_txn_db.raw_transactions

UNION ALL

SELECT
    'staging' AS layer,
    COUNT(*) AS row_count
FROM credit_txn_db.staging_transactions_cleaned;


-- Null check on staging critical columns
SELECT
    SUM(CASE WHEN trans_num IS NULL THEN 1 ELSE 0 END) AS null_trans_num,
    SUM(CASE WHEN cc_num IS NULL THEN 1 ELSE 0 END) AS null_cc_num,
    SUM(CASE WHEN amt IS NULL THEN 1 ELSE 0 END) AS null_amt,
    SUM(CASE WHEN merchant IS NULL THEN 1 ELSE 0 END) AS null_merchant,
    SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) AS null_category,
    SUM(CASE WHEN trans_date_trans_time IS NULL THEN 1 ELSE 0 END) AS null_timestamp
FROM credit_txn_db.staging_transactions_cleaned;


-- Duplicate check on staging
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT trans_num) AS unique_trans_nums,
    COUNT(*) - COUNT(DISTINCT trans_num) AS duplicates
FROM credit_txn_db.staging_transactions_cleaned;


-- Amount sanity check
SELECT
    MIN(amt) AS min_amount,
    MAX(amt) AS max_amount,
    ROUND(AVG(amt), 2) AS avg_amount,
    APPROX_PERCENTILE(amt, 0.5) AS median_amount,
    APPROX_PERCENTILE(amt, 0.99) AS p99_amount
FROM credit_txn_db.staging_transactions_cleaned;