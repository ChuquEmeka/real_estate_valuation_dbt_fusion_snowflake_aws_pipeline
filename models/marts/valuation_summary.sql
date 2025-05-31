select
    p.property_id,
    p.borough,
    p.property_type,
    p.size_sqm,
    p.total_rooms,
    max(f.transaction_date) as last_transaction_date,
    max(f.price_per_sqm) * p.size_sqm as valuation_from_trend,
    {{ valuation_income_approach("median_income", cap_rate()) }}
    as valuation_income_approach
from {{ ref("dim_properties") }} p
join {{ ref("fct_transactions") }} f on p.property_id = f.property_id
group by
    p.property_id,
    p.borough,
    p.property_type,
    p.size_sqm,
    p.total_rooms,
    p.median_income
