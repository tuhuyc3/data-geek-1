{% macro extract_string_param(param, sign = '= ', nested = 'event_params') -%}
(select value.string_value from unnest({{nested}}) where key {{sign}}{{param}})
{%- endmacro %}

