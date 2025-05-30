-- Transactions must link to existing properties
SELECT *
FROM {{ ref('fct_transactions') }} ft
LEFT JOIN {{ ref('dim_properties') }} dp
  ON ft.property_id = dp.property_id
WHERE dp.property_id IS NULL
