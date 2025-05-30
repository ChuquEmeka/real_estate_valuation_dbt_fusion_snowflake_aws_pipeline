-- Filter out construction years before 1800 or after current year
SELECT *
FROM {{ ref('dim_properties') }}
WHERE construction_year < 1960
   OR construction_year > EXTRACT(YEAR FROM CURRENT_DATE)
