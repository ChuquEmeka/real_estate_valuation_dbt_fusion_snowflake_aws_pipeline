-- Ensure no negative sale prices exist
SELECT *
FROM {{ ref('fct_transactions') }}
WHERE sale_price < 0
