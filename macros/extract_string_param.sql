{% macro extract_string_param(param_name, multi_name = '= ') -%}
(select value.string_value from unnest(event_params) where key {{multi_name}}{{param_name}})
{%- endmacro %}
