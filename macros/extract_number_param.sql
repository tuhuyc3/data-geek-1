{% macro extract_number_param(param, sign = '= ', nested = 'event_params') -%}
(select  coalesce(coalesce(value.int_value,value.float_value),value.double_value) from unnest({{nested}}) where key  {{sign}}{{param}})
{%- endmacro %}

