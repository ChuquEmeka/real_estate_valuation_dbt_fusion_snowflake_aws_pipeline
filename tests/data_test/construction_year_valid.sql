-- Filter out construction years before 1800 or after current year
select *
from {{ ref("dim_properties") }}
where construction_year < 1960 or construction_year > extract(year from current_date)
