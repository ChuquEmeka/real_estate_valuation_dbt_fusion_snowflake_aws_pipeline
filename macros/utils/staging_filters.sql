{% macro filter_valid_property(
    construction_col='construction_year',
    min_year=1960,
    sale_price_col='sale_price',
    price_per_sqm_col='price_per_sqm'
) %}

(
    {{ construction_col }} BETWEEN {{ min_year }} AND EXTRACT(YEAR FROM CURRENT_DATE)
    AND {{ sale_price_col }} > 0
    AND {{ price_per_sqm_col }} > 0
)

{% endmacro %}