{% macro generate_schema_name(custom_schema_name, node) %}
    {%- set default_schema = target.schema | upper -%}

    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | upper }}

    {%- elif 'analysis' in node.path -%}
        ANALYSIS

    {%- elif 'core' in node.path or 'marts' in node.path -%}
        CORE

    {%- elif 'staging' in node.path or 'base' in node.path -%}
        STAGING

    {%- elif 'quarantine' in node.path -%}
        QUARANTINE

    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{% endmacro %}
