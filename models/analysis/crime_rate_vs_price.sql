{{ config(materialized="table") }}

with
    borough_price_crime as (
        select
            p.borough,
            avg(f.price_per_sqm) as avg_price_per_sqm,
            avg(p.crime_rate) as avg_crime_rate,
            count(distinct p.property_id) as properties_count
        from {{ ref("fct_transactions") }} f
        join {{ ref("dim_properties") }} p on f.property_id = p.property_id
        group by p.borough
    )

select
    borough,
    avg_price_per_sqm,
    avg_crime_rate,
    properties_count,
    case
        when avg_crime_rate = 0
        then null
        else round(avg_price_per_sqm / avg_crime_rate, 2)
    end as price_per_crime_rate_ratio
from borough_price_crime
order by avg_crime_rate desc
