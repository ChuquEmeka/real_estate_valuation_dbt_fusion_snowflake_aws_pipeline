{{ config(
    materialized='table'
) }}

WITH latest_transactions AS (
  SELECT
    property_id,
    MAX(transaction_date) AS max_transaction_date
  FROM {{ ref('fct_transactions') }}
  GROUP BY property_id
)

SELECT
  d.property_id,
  d.borough,
  d.property_type,
  d.size_sqm,
  d.total_rooms,
  f.transaction_date,
  f.sale_price,
  f.price_per_sqm
FROM {{ ref('dim_properties') }} d
JOIN {{ ref('fct_transactions') }} f
  ON d.property_id = f.property_id
JOIN latest_transactions lt
  ON f.property_id = lt.property_id
  AND f.transaction_date = lt.max_transaction_date
