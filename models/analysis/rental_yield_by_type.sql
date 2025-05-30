{{ config(materialized='table') }}

SELECT
  p.property_type,
  borough,
  AVG(rental_yield) AS avg_rental_yield,
  COUNT(DISTINCT p.property_id) AS property_count
FROM {{ ref('dim_properties') }} p
JOIN {{ ref('fct_transactions') }} f ON p.property_id = f.property_id
GROUP BY p.property_type, borough
ORDER BY borough, avg_rental_yield DESC
