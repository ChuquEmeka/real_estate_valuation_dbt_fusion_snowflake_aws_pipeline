{{ config(materialized="table") }}

select
    p.property_type,
    borough,
    avg(rental_yield) as avg_rental_yield,
    count(distinct p.property_id) as property_count
from {{ ref("dim_properties") }} p
join {{ ref("fct_transactions") }} f on p.property_id = f.property_id
group by p.property_type, borough
order by borough, avg_rental_yield desc
