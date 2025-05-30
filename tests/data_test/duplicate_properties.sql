-- No duplicate property_id entries in dim_properties
SELECT property_id, COUNT(*)
FROM {{ ref('dim_properties') }}
GROUP BY property_id
HAVING COUNT(*) > 1
