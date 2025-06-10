{% macro filter_incremental_rows(updated_at_col) %}
    {% if is_incremental() %}
        {{ updated_at_col }} > (SELECT MAX({{ updated_at_col }}) FROM {{ this }})
    {% else %}
        true -- no filtering for full refresh
    {% endif %}
{% endmacro %}

{% macro safe_divide(numerator, denominator) %}
  CASE WHEN {{ denominator }} != 0 THEN {{ numerator }} / {{ denominator }} ELSE NULL END
{% endmacro %}

{% macro coalesce_multiple(args) %}
  COALESCE({{ args | join(', ') }})
{% endmacro %}

{% macro current_timestamp_utc() %}
  CURRENT_TIMESTAMP() AT TIME ZONE 'UTC'
{% endmacro %}

{% macro date_trunc_to_month(col) %}
  DATE_TRUNC('month', {{ col }})
{% endmacro %}

{% macro date_trunc_to_year(col) %}
  DATE_TRUNC('year', {{ col }})
{% endmacro %}
