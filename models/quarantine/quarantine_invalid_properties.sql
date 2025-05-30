{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

WITH flattened_transactions AS (
    SELECT
        raw_data:property_id::STRING AS property_id,
        raw_data:address.street::STRING AS street,
        raw_data:address.postal_code::STRING AS postal_code,
        raw_data:address.borough::STRING AS borough,
        raw_data:coordinates.latitude::FLOAT AS latitude,
        raw_data:coordinates.longitude::FLOAT AS longitude,
        raw_data:property_type::STRING AS property_type,
        raw_data:size_sqm::FLOAT AS size_sqm,
        raw_data:lot_size_sqm::FLOAT AS lot_size_sqm,
        raw_data:rooms.bedrooms::INT AS bedrooms,
        raw_data:rooms.bathrooms::INT AS bathrooms,
        raw_data:rooms.total_rooms::INT AS total_rooms,
        raw_data:construction_year::INT AS construction_year,
        raw_data:renovation_year::INT AS renovation_year,
        raw_data:condition::STRING AS condition,
        raw_data:features.balcony::BOOLEAN AS has_balcony,
        raw_data:features.elevator::BOOLEAN AS has_elevator,
        raw_data:features.heating_type::STRING AS heating_type,
        raw_data:features.parking::BOOLEAN AS has_parking,
        raw_data:energy_rating::STRING AS energy_rating,
        raw_data:energy_consumption_kwh_m2::FLOAT AS energy_consumption_kwh_m2,
        raw_data:proximity.public_transport_m::INT AS dist_transport,
        raw_data:proximity.schools_m::INT AS dist_schools,
        raw_data:proximity.parks_m::INT AS dist_parks,
        raw_data:neighborhood_metrics.median_income_eur::INT AS median_income,
        raw_data:neighborhood_metrics.population_density_per_sqm::INT AS pop_density,
        raw_data:neighborhood_metrics.crime_rate_per_1000::FLOAT AS crime_rate,
        raw_data:market_trends.avg_price_per_sqm_2003_eur::FLOAT AS avg_ppsqm_2003,
        raw_data:market_trends.avg_price_per_sqm_2025_eur::FLOAT AS avg_ppsqm_2025,
        raw_data:market_trends.cap_rate_percent::FLOAT AS cap_rate,
        raw_data:market_trends.rental_yield_percent::FLOAT AS rental_yield,
        flattened.value:transaction_id::STRING AS transaction_id,
        flattened.value:sale_price_eur::FLOAT AS sale_price,
        flattened.value:transaction_date::DATE AS transaction_date,
        flattened.value:price_per_sqm_eur::FLOAT AS price_per_sqm,
        flattened.value:market_conditions.interest_rate_percent::FLOAT AS interest_rate,
        flattened.value:market_conditions.demand_index::INT AS demand_index
    FROM {{ source('staging', 'raw_properties') }},
         LATERAL FLATTEN(input => raw_data:transaction_history) AS flattened
)

SELECT *,
    CASE
        WHEN construction_year IS NULL THEN 'Missing construction year'
        WHEN construction_year < 1960 THEN 'Construction year too old'
        WHEN construction_year > EXTRACT(YEAR FROM CURRENT_DATE) THEN 'Construction year in the future'
        WHEN sale_price IS NULL OR sale_price <= 0 THEN 'Invalid or missing sale price'
        WHEN price_per_sqm IS NULL OR price_per_sqm <= 0 THEN 'Invalid or missing price per sqm'
        ELSE 'Unknown reason'
    END AS quarantine_reason
FROM flattened_transactions
WHERE NOT ({{ filter_valid_property() }})
    AND {{ filter_dev_data_by_year('transaction_date', 2024) }}
    AND {{ filter_incremental_rows('transaction_date') }}