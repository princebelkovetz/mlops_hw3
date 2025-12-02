-- models/marts/mart_customer_risk_profile.sql

SELECT
    full_name,
    COUNT(*) AS transaction_count,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate,
    AVG(amount) AS avg_check,
    CASE
        WHEN SUM(is_fraud) > 0 
             AND COUNT(CASE WHEN amount_bucket = 'very_large' THEN 1 END) > 0 
             THEN 'HIGH'
        WHEN SUM(is_fraud) > 0 
             OR COUNT(CASE WHEN amount_bucket IN ('large', 'very_large') THEN 1 END) > 0 
             THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_level
FROM {{ ref('stg_transactions') }}
GROUP BY full_name