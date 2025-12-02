-- models/marts/mart_fraud_by_state.sql
SELECT
    us_state,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT full_name) AS unique_customers, 
    COUNT(DISTINCT merch) AS unique_merchants,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate,
    SUM(amount) AS total_amount
FROM {{ ref('stg_transactions') }}
GROUP BY us_state