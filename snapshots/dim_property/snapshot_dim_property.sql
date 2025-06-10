-- snapshots/snapshot_dim_property.sql

{% snapshot snapshot_dim_property %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='property_id',
        strategy='check',
        check_cols=[
            'renovation_year',
            'condition',
            'has_balcony',
            'has_elevator',
            'has_parking',
            'heating_type',
            'energy_rating',
            'energy_consumption_kwh_m2',
            'cap_rate',
            'median_income',
            'pop_density',
            'crime_rate'
        ]
    )
}}

SELECT * FROM {{ ref('dim_properties') }}

{% endsnapshot %}
