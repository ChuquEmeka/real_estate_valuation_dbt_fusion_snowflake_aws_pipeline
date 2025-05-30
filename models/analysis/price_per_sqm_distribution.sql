{{ config(materialized='table') }}

WITH price_stats AS (
  SELECT
    borough,
    property_type,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price_per_sqm) AS price_per_sqm_p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY price_per_sqm) AS price_per_sqm_median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price_per_sqm) AS price_per_sqm_p75,
    AVG(price_per_sqm) AS avg_price_per_sqm,
    COUNT(*) AS total_transactions
  FROM {{ ref('fct_transactions') }} f
  JOIN {{ ref('dim_properties') }} p ON f.property_id = p.property_id
  GROUP BY borough, property_type
)

SELECT * FROM price_stats
ORDER BY borough, property_type
