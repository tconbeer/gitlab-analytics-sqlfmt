{% macro unpack_unstructured_event(unstruct_columns_list, match_text, field_prefix) -%}

{%- for column in unstruct_columns_list %}
case
    -- Mask these as they contain sensitive data.
    when event_name in ('value', 'elements')
    then 'masked'
    when event_name = '{{ match_text }}'
    then try_parse_json(unstruct_event)['data']['data']['{{ column }}']
    else null
end as {{ field_prefix }}_{{ column }}
{%- if not loop.last %}, {% endif %}
{% endfor -%}

{%- endmacro %}
