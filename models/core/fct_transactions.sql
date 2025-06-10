{{ config(materialized="incremental", unique_key="transaction_id") }}

select
    transaction_id,
    rental_yield,
    property_id,
    transaction_date,
    sale_price,
    price_per_sqm,
    interest_rate,
    demand_index
from {{ ref("stg_properties") }}
where {{ filter_incremental_rows("transaction_date") }}
