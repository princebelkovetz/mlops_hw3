SELECT
    cat_id,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) AS fraud_rate,
    SUM(amount) AS total_amount
FROM {{ ref('stg_transactions') }}
GROUP BY cat_id