-- Price per sqm must be greater than 0
select * from {{ ref("fct_transactions") }} where price_per_sqm <= 0
