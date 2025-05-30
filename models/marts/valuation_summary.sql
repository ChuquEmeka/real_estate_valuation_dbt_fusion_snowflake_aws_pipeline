SELECT
  p.property_id,
  p.borough,
  p.property_type,
  p.size_sqm,
  p.total_rooms,
  MAX(f.transaction_date) AS last_transaction_date,
  MAX(f.price_per_sqm) * p.size_sqm AS valuation_from_trend,
  {{ valuation_income_approach("median_income", cap_rate()) }} AS valuation_income_approach
FROM {{ ref('dim_properties') }} p
JOIN {{ ref('fct_transactions') }} f 
  ON p.property_id = f.property_id
GROUP BY
  p.property_id,
  p.borough,
  p.property_type,
  p.size_sqm,
  p.total_rooms,
  p.median_income
