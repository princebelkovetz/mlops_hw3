-- dbt/models/staging/stg_transactions.sql

SELECT
    transaction_time,
    toDate(transaction_time) AS transaction_date,
    toHour(transaction_time) AS transaction_hour,
    toDayOfWeek(transaction_time) AS day_of_week,
    merch,
    cat_id,
    amount,
    {{ amount_bucket('amount') }} AS amount_bucket,
    concat(name_1, ' ', name_2) AS full_name,
    gender,
    us_state,
    target = 1 AS is_fraud
FROM {{ source('raw', 'transactions') }}
WHERE amount > 0