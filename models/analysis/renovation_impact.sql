{{ config(materialized="table") }}

with
    renovation_effect as (
        select
            p.borough,
            p.property_type,
            case
                when p.renovation_year is null
                then 'No Renovation'
                when p.renovation_year >= year(current_date) - 5
                then 'Renovated Last 5 Years'
                else 'Renovated > 5 Years Ago'
            end as renovation_status,
            avg(f.price_per_sqm) as avg_price_per_sqm,
            count(distinct p.property_id) as properties_count
        from {{ ref("fct_transactions") }} f
        join {{ ref("dim_properties") }} p on f.property_id = p.property_id
        group by 1, 2, 3
    )

select borough, property_type, renovation_status, avg_price_per_sqm, properties_count
from renovation_effect
order by borough, property_type, renovation_status
