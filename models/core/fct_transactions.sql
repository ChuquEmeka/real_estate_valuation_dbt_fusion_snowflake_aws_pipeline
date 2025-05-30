{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

SELECT
  transaction_id,
  rental_yield,
  property_id,
  transaction_date,
  sale_price,
  price_per_sqm,
  interest_rate,
  demand_index
FROM {{ ref('stg_properties') }}
WHERE {{ filter_incremental_rows('transaction_date') }}
