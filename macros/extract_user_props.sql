{% macro extract_user_props(param, sign = '= ') -%}
coalesce ((select value.string_value from unnest(event_params) where key {{sign}}{{param}}),(select value.string_value from unnest(user_properties) where key {{sign}}{{param}}))
{%- endmacro %}


