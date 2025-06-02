{{ config(materialized="incremental", unique_key="transaction_id") }}

with
    flattened_transactions as (
        {{ flatten_property_transactions() }}
    )

select
    *,
    case
        when construction_year is null
        then 'Missing construction year'
        when construction_year < 1960
        then 'Construction year too old'
        when construction_year > extract(year from current_date)
        then 'Construction year in the future'
        when sale_price is null or sale_price <= 0
        then 'Invalid or missing sale price'
        when price_per_sqm is null or price_per_sqm <= 0
        then 'Invalid or missing price per sqm'
        else 'Unknown reason'
    end as quarantine_reason
from flattened_transactions
where
    not ({{ filter_valid_property() }})
    and {{ filter_dev_data_by_year("transaction_date", 2024) }}
    and {{ filter_incremental_rows("transaction_date") }}
