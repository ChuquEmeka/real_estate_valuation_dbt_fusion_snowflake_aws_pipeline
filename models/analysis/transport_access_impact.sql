{{ config(materialized='table') }}

WITH borough_transport AS (
  SELECT
    p.borough,
    AVG(f.price_per_sqm) AS avg_price_per_sqm,
    AVG(p.dist_transport) AS avg_dist_transport,
    COUNT(DISTINCT p.property_id) AS properties_count
  FROM {{ ref('fct_transactions') }} f
  JOIN {{ ref('dim_properties') }} p ON f.property_id = p.property_id
  GROUP BY p.borough
),

distance_price_correlation AS (
  SELECT
    borough,
    avg_price_per_sqm,
    avg_dist_transport,
    properties_count,
    CASE 
      WHEN avg_dist_transport = 0 THEN NULL
      ELSE ROUND(avg_price_per_sqm / avg_dist_transport, 2)
    END AS price_per_meter_transport_access_ratio
  FROM borough_transport
)

SELECT * FROM distance_price_correlation
ORDER BY price_per_meter_transport_access_ratio DESC
