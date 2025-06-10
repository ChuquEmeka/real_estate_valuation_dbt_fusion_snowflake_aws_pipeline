{% macro cap_rate() %}
    -- Capitalization rate, used in income-based valuation
    0.05  -- You can parameterize this later
{% endmacro %}

{% macro yield_multiplier() %}
    -- Yield multiplier for the income approach
    0.03  -- This could come from a seed table or environment config
{% endmacro %}

{% macro valuation_income_approach(median_income_col, cap_rate_value=None) %}
    -- Valuation using the income approach:
    -- Formula: valuation = median_income * yield_multiplier / cap_rate
    CASE 
        WHEN {{ cap_rate_value if cap_rate_value is not none else cap_rate() }} = 0 THEN NULL
        ELSE {{ median_income_col }} * {{ yield_multiplier() }} / {{ cap_rate_value if cap_rate_value is not none else cap_rate() }}
    END
{% endmacro %}

{% macro valuation_trend(price_per_sqm_col, size_sqm_col) %}
    -- Valuation using the trend approach: price_per_sqm * size_sqm
    ({{ price_per_sqm_col }} * {{ size_sqm_col }})
{% endmacro %}
