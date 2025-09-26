{% test unique_combination(model, column_names) %}
    {%- if column_names is string -%}
        {%- set column_list = [column_names] -%}
    {%- else -%}
        {%- set column_list = column_names -%}
    {%- endif -%}

    select
      {{ column_list | join(', ') }}
    from {{ model }}
    group by {{ column_list | join(', ') }}
    having count(*) > 1
{% endtest %}
