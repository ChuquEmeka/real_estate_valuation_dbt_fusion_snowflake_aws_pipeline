-- Price per sqm must be greater than 0
SELECT *
FROM {{ ref('fct_transactions') }}
WHERE price_per_sqm <= 0
