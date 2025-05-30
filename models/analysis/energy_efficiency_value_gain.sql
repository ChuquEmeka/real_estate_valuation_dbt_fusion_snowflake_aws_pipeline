{{ config(materialized='table') }}

WITH energy_effect AS (
  SELECT
    p.borough,
    p.property_type,
    p.energy_rating,
    AVG(f.price_per_sqm) AS avg_price_per_sqm,
    COUNT(DISTINCT p.property_id) AS properties_count
  FROM {{ ref('fct_transactions') }} f
  JOIN {{ ref('dim_properties') }} p ON f.property_id = p.property_id
  GROUP BY 1, 2, 3
)

SELECT
  borough,
  property_type,
  energy_rating,
  avg_price_per_sqm,
  properties_count
FROM energy_effect
ORDER BY borough, property_type, energy_rating
