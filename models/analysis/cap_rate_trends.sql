{{ config(materialized="table") }}

select
    p.borough,
    date_trunc('year', f.transaction_date) as year,
    avg(p.cap_rate) as avg_cap_rate,
    count(distinct f.property_id) as property_count
from {{ ref("fct_transactions") }} f
join {{ ref("dim_properties") }} p on f.property_id = p.property_id
where p.cap_rate is not null
group by p.borough, year
order by p.borough, year
