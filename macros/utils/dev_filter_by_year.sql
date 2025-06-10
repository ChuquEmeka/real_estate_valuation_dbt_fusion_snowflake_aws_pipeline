{% macro filter_dev_data_by_year(column_name='transaction_date', year=2024) %}
    {% if target.name == 'dev' %}
        date_trunc('year', {{ column_name }}) = date_from_parts({{ year }}, 1, 1)
    {% else %}
        true -- no filtering in prod
    {% endif %}
{% endmacro %}