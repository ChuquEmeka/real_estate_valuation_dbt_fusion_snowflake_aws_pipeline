{{ config(materialized='table') }}

SELECT
  p.borough,
  DATE_TRUNC('year', f.transaction_date) AS year,
  AVG(p.cap_rate) AS avg_cap_rate,
  COUNT(DISTINCT f.property_id) AS property_count
FROM {{ ref('fct_transactions') }} f
JOIN {{ ref('dim_properties') }} p ON f.property_id = p.property_id
WHERE p.cap_rate IS NOT NULL
GROUP BY p.borough, year
ORDER BY p.borough, year
