{{ config(materialized="incremental", unique_key="transaction_id") }}

with
    flattened_transactions as (
        select
            raw_data:property_id::string as property_id,
            raw_data:address.street::string as street,
            raw_data:address.postal_code::string as postal_code,
            raw_data:address.borough::string as borough,
            raw_data:coordinates.latitude::float as latitude,
            raw_data:coordinates.longitude::float as longitude,
            raw_data:property_type::string as property_type,
            raw_data:size_sqm::float as size_sqm,
            raw_data:lot_size_sqm::float as lot_size_sqm,
            raw_data:rooms.bedrooms::int as bedrooms,
            raw_data:rooms.bathrooms::int as bathrooms,
            raw_data:rooms.total_rooms::int as total_rooms,
            raw_data:construction_year::int as construction_year,
            raw_data:renovation_year::int as renovation_year,
            raw_data:condition::string as condition,
            raw_data:features.balcony::boolean as has_balcony,
            raw_data:features.elevator::boolean as has_elevator,
            raw_data:features.heating_type::string as heating_type,
            raw_data:features.parking::boolean as has_parking,
            raw_data:energy_rating::string as energy_rating,
            raw_data:energy_consumption_kwh_m2::float as energy_consumption_kwh_m2,
            raw_data:proximity.public_transport_m::int as dist_transport,
            raw_data:proximity.schools_m::int as dist_schools,
            raw_data:proximity.parks_m::int as dist_parks,
            raw_data:neighborhood_metrics.median_income_eur::int as median_income,
            raw_data:neighborhood_metrics.population_density_per_sqm::int
            as pop_density,
            raw_data:neighborhood_metrics.crime_rate_per_1000::float as crime_rate,
            raw_data:market_trends.avg_price_per_sqm_2003_eur::float as avg_ppsqm_2003,
            raw_data:market_trends.avg_price_per_sqm_2025_eur::float as avg_ppsqm_2025,
            raw_data:market_trends.cap_rate_percent::float as cap_rate,
            raw_data:market_trends.rental_yield_percent::float as rental_yield,
            flattened.value:transaction_id::string as transaction_id,
            flattened.value:sale_price_eur::float as sale_price,
            flattened.value:transaction_date::date as transaction_date,
            flattened.value:price_per_sqm_eur::float as price_per_sqm,
            flattened.value:market_conditions.interest_rate_percent::float
            as interest_rate,
            flattened.value:market_conditions.demand_index::int as demand_index
        from
            {{ source("staging", "raw_properties") }},
            lateral flatten(input => raw_data:transaction_history) as flattened
    )

select
    *,
    case
        when construction_year is null
        then 'Missing construction year'
        when construction_year < 1960
        then 'Construction year too old'
        when construction_year > extract(year from current_date)
        then 'Construction year in the future'
        when sale_price is null or sale_price <= 0
        then 'Invalid or missing sale price'
        when price_per_sqm is null or price_per_sqm <= 0
        then 'Invalid or missing price per sqm'
        else 'Unknown reason'
    end as quarantine_reason
from flattened_transactions
where
    not ({{ filter_valid_property() }})
    and {{ filter_dev_data_by_year("transaction_date", 2024) }}
    and {{ filter_incremental_rows("transaction_date") }}
