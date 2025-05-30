{{ config(materialized='table') }}

WITH monthly_prices AS (
  SELECT
    DATE_TRUNC('month', transaction_date) AS month,
    borough,
    property_type,
    AVG(price_per_sqm) AS avg_price_per_sqm,
    COUNT(*) AS transactions_count
  FROM {{ ref('fct_transactions') }} f
  JOIN {{ ref('dim_properties') }} p ON f.property_id = p.property_id
  GROUP BY 1, 2, 3
),

price_trend AS (
  SELECT
    month,
    borough,
    property_type,
    avg_price_per_sqm,
    LAG(avg_price_per_sqm) OVER (PARTITION BY borough, property_type ORDER BY month) AS prev_avg_price,
    (avg_price_per_sqm - LAG(avg_price_per_sqm) OVER (PARTITION BY borough, property_type ORDER BY month)) / NULLIF(LAG(avg_price_per_sqm) OVER (PARTITION BY borough, property_type ORDER BY month), 0) AS month_pct_change
  FROM monthly_prices
)

SELECT
  month,
  borough,
  property_type,
  avg_price_per_sqm,
  month_pct_change
FROM price_trend
ORDER BY borough, property_type, month
