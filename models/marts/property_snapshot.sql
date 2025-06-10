{{ config(materialized="table") }}

with
    latest_transactions as (
        select property_id, max(transaction_date) as max_transaction_date
        from {{ ref("fct_transactions") }}
        group by property_id
    )

select
    d.property_id,
    d.borough,
    d.property_type,
    d.size_sqm,
    d.total_rooms,
    f.transaction_date,
    f.sale_price,
    f.price_per_sqm
from {{ ref("dim_properties") }} d
join {{ ref("fct_transactions") }} f on d.property_id = f.property_id
join
    latest_transactions lt
    on f.property_id = lt.property_id
    and f.transaction_date = lt.max_transaction_date
