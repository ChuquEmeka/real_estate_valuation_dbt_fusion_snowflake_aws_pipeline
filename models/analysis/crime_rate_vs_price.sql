{{ config(materialized='table') }}

WITH borough_price_crime AS (
  SELECT
    p.borough,
    AVG(f.price_per_sqm) AS avg_price_per_sqm,
    AVG(p.crime_rate) AS avg_crime_rate,
    COUNT(DISTINCT p.property_id) AS properties_count
  FROM {{ ref('fct_transactions') }} f
  JOIN {{ ref('dim_properties') }} p ON f.property_id = p.property_id
  GROUP BY p.borough
)

SELECT
  borough,
  avg_price_per_sqm,
  avg_crime_rate,
  properties_count,
  CASE 
    WHEN avg_crime_rate = 0 THEN NULL
    ELSE ROUND(avg_price_per_sqm / avg_crime_rate, 2)
  END AS price_per_crime_rate_ratio
FROM borough_price_crime
ORDER BY avg_crime_rate DESC
