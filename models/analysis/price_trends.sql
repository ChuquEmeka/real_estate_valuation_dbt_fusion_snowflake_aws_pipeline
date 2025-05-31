{{ config(materialized="table") }}

with
    monthly_prices as (
        select
            date_trunc('month', transaction_date) as month,
            borough,
            property_type,
            avg(price_per_sqm) as avg_price_per_sqm,
            count(*) as transactions_count
        from {{ ref("fct_transactions") }} f
        join {{ ref("dim_properties") }} p on f.property_id = p.property_id
        group by 1, 2, 3
    ),

    price_trend as (
        select
            month,
            borough,
            property_type,
            avg_price_per_sqm,
            lag(avg_price_per_sqm) over (
                partition by borough, property_type order by month
            ) as prev_avg_price,
            (
                avg_price_per_sqm - lag(avg_price_per_sqm) over (
                    partition by borough, property_type order by month
                )
            ) / nullif(
                lag(avg_price_per_sqm) over (
                    partition by borough, property_type order by month
                ),
                0
            ) as month_pct_change
        from monthly_prices
    )

select month, borough, property_type, avg_price_per_sqm, month_pct_change
from price_trend
order by borough, property_type, month
