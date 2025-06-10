{{ config(materialized="table") }}

with
    price_stats as (
        select
            borough,
            property_type,
            percentile_cont(0.25) within group (
                order by price_per_sqm
            ) as price_per_sqm_p25,
            percentile_cont(0.50) within group (
                order by price_per_sqm
            ) as price_per_sqm_median,
            percentile_cont(0.75) within group (
                order by price_per_sqm
            ) as price_per_sqm_p75,
            avg(price_per_sqm) as avg_price_per_sqm,
            count(*) as total_transactions
        from {{ ref("fct_transactions") }} f
        join {{ ref("dim_properties") }} p on f.property_id = p.property_id
        group by borough, property_type
    )

select *
from price_stats
order by borough, property_type
