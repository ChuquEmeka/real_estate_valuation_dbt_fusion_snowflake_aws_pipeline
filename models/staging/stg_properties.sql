{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

{{ flatten_property_transactions() }}

WHERE {{ filter_valid_property() }}
    AND {{ filter_dev_data_by_year('transaction_date', 2024) }}
    AND {{ filter_incremental_rows('transaction_date') }}