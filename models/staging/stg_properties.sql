{{ config(materialized="incremental", unique_key="transaction_id") }}

{{ flatten_property_transactions() }}

where
    {{ filter_valid_property() }}
    and {{ filter_dev_data_by_year("transaction_date", 2024) }}
    and {{ filter_incremental_rows("transaction_date") }}
