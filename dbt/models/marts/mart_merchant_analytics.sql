-- models/marts/mart_merchant_analytics.sql
SELECT
    merch,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate,
    CASE
        WHEN SUM(is_fraud) * 100.0 / COUNT(*) > 10 THEN 1
        ELSE 0
    END AS is_suspicious
FROM {{ ref('stg_transactions') }}
GROUP BY merch
HAVING transaction_count >= 10  