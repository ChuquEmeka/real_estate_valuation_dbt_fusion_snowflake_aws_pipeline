{{ config(materialized="table") }}

with
    energy_effect as (
        select
            p.borough,
            p.property_type,
            p.energy_rating,
            avg(f.price_per_sqm) as avg_price_per_sqm,
            count(distinct p.property_id) as properties_count
        from {{ ref("fct_transactions") }} f
        join {{ ref("dim_properties") }} p on f.property_id = p.property_id
        group by 1, 2, 3
    )

select borough, property_type, energy_rating, avg_price_per_sqm, properties_count
from energy_effect
order by borough, property_type, energy_rating
