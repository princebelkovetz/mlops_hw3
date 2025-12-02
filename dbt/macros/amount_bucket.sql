-- dbt/macros/amount_bucket.sql
{% macro amount_bucket(amount_column) %}
    CASE
        WHEN {{ amount_column }} < 25 THEN 'small'
        WHEN {{ amount_column }} < 100 THEN 'medium'
        WHEN {{ amount_column }} < 500 THEN 'large'
        ELSE 'very_large'
    END
{% endmacro %}