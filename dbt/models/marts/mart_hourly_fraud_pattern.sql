-- models/marts/mart_hourly_fraud_pattern.sql
SELECT
    day_of_week,
    transaction_hour,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate
FROM {{ ref('stg_transactions') }}
GROUP BY day_of_week, transaction_hour
ORDER BY day_of_week, transaction_hour