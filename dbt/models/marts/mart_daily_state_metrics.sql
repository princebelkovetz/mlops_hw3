SELECT
    transaction_date,
    us_state,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_check,
    quantile(0.95)(amount) AS p95_amount,
    SUM(CASE WHEN amount_bucket IN ('large', 'very_large') THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_large_transactions
FROM {{ ref('stg_transactions') }}
GROUP BY transaction_date, us_state