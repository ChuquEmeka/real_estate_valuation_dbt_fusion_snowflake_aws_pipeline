-- Ensure no negative sale prices exist
select * from {{ ref("fct_transactions") }} where sale_price < 0
