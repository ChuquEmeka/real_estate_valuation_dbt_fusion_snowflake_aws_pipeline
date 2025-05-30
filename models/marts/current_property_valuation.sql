{{ config(
    materialized='incremental',
    unique_key='property_id'
) }}

WITH latest_transactions AS (
  SELECT
    property_id,
    MAX(transaction_date) AS latest_transaction_date
  FROM {{ ref('fct_transactions') }}
  GROUP BY property_id
),

trend_valuation AS (
  SELECT
    p.property_id,
    MAX(f.transaction_date) AS last_transaction_date,
    MAX(f.price_per_sqm) * p.size_sqm AS valuation_trend
  FROM {{ ref('dim_properties') }} p
  JOIN {{ ref('fct_transactions') }} f ON p.property_id = f.property_id
  GROUP BY p.property_id, p.size_sqm
),

income_valuation AS (
  SELECT
    property_id,
    {{ valuation_income_approach('median_income') }} AS valuation_income
  FROM {{ ref('dim_properties') }}
)

SELECT
  p.property_id,
  p.borough,
  p.property_type,
  p.size_sqm,
  p.total_rooms,
  lt.latest_transaction_date,
  t.valuation_trend,
  i.valuation_income
FROM {{ ref('dim_properties') }} p
LEFT JOIN latest_transactions lt ON p.property_id = lt.property_id
LEFT JOIN trend_valuation t ON p.property_id = t.property_id
LEFT JOIN income_valuation i ON p.property_id = i.property_id

{% if is_incremental() %}
WHERE lt.latest_transaction_date > (
    SELECT MAX(latest_transaction_date)
    FROM {{ this }}
)
{% endif %}
