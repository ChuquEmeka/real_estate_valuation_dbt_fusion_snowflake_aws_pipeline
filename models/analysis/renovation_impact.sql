{{ config(materialized='table') }}

WITH renovation_effect AS (
  SELECT
    p.borough,
    p.property_type,
    CASE
      WHEN p.renovation_year IS NULL THEN 'No Renovation'
      WHEN p.renovation_year >= YEAR(CURRENT_DATE) - 5 THEN 'Renovated Last 5 Years'
      ELSE 'Renovated > 5 Years Ago'
    END AS renovation_status,
    AVG(f.price_per_sqm) AS avg_price_per_sqm,
    COUNT(DISTINCT p.property_id) AS properties_count
  FROM {{ ref('fct_transactions') }} f
  JOIN {{ ref('dim_properties') }} p ON f.property_id = p.property_id
  GROUP BY 1, 2, 3
)

SELECT
  borough,
  property_type,
  renovation_status,
  avg_price_per_sqm,
  properties_count
FROM renovation_effect
ORDER BY borough, property_type, renovation_status
