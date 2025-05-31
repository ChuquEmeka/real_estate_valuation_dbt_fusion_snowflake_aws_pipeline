{{ config(materialized="table") }}

with
    borough_transport as (
        select
            p.borough,
            avg(f.price_per_sqm) as avg_price_per_sqm,
            avg(p.dist_transport) as avg_dist_transport,
            count(distinct p.property_id) as properties_count
        from {{ ref("fct_transactions") }} f
        join {{ ref("dim_properties") }} p on f.property_id = p.property_id
        group by p.borough
    ),

    distance_price_correlation as (
        select
            borough,
            avg_price_per_sqm,
            avg_dist_transport,
            properties_count,
            case
                when avg_dist_transport = 0
                then null
                else round(avg_price_per_sqm / avg_dist_transport, 2)
            end as price_per_meter_transport_access_ratio
        from borough_transport
    )

select *
from distance_price_correlation
order by price_per_meter_transport_access_ratio desc
