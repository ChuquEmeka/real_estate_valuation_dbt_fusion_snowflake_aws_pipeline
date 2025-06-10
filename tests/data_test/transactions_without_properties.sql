-- Transactions must link to existing properties
select *
from {{ ref("fct_transactions") }} ft
left join {{ ref("dim_properties") }} dp on ft.property_id = dp.property_id
where dp.property_id is null
