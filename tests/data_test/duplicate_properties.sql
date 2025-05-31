-- No duplicate property_id entries in dim_properties
select property_id, count(*)
from {{ ref("dim_properties") }}
group by property_id
having count(*) > 1
