{{ config(materialized="incremental", unique_key="property_id") }}

with
    latest_transactions as (
        select property_id, max(transaction_date) as latest_transaction_date
        from {{ ref("fct_transactions") }}
        group by property_id
    ),

    trend_valuation as (
        select
            p.property_id,
            max(f.transaction_date) as last_transaction_date,
            max(f.price_per_sqm) * p.size_sqm as valuation_trend
        from {{ ref("dim_properties") }} p
        join {{ ref("fct_transactions") }} f on p.property_id = f.property_id
        group by p.property_id, p.size_sqm
    ),

    income_valuation as (
        select
            property_id,
            {{ valuation_income_approach("median_income") }} as valuation_income
        from {{ ref("dim_properties") }}
    )

select
    p.property_id,
    p.borough,
    p.property_type,
    p.size_sqm,
    p.total_rooms,
    lt.latest_transaction_date,
    t.valuation_trend,
    i.valuation_income
from {{ ref("dim_properties") }} p
left join latest_transactions lt on p.property_id = lt.property_id
left join trend_valuation t on p.property_id = t.property_id
left join income_valuation i on p.property_id = i.property_id

{% if is_incremental() %}
    where
        lt.latest_transaction_date
        > (select max(latest_transaction_date) from {{ this }})
{% endif %}
