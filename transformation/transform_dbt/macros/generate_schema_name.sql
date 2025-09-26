{# Prevent dbt from prefixing schema (no more silver_silver / silver_gold) #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ return(custom_schema_name if custom_schema_name is not none and custom_schema_name|length > 0
            else target.schema) }}
{%- endmacro %}