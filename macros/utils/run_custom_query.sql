{% macro run_custom_query() %}
    {% set query %}
        SELECT
            transaction_id AS customer_id,
            CONCAT(street, ' ', borough) AS customer_name,
            COUNT(*) AS count_lifetime_order,
            MIN(transaction_date) AS first_ordered_at,
            MAX(transaction_date) AS last_ordered_at,
            SUM(sale_price) AS lifetime_value
        FROM {{ ref('stg_properties') }}
        GROUP BY transaction_id, CONCAT(street, ' ', borough)
        LIMIT 10
    {% endset %}
    {% set results = run_query(query) %}
    {% if execute %}
        {% do results.print_table() %}
    {% endif %}
{% endmacro %}