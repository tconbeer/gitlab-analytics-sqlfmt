{%- macro resource_event_action_type(resource_event_action_type_id) -%}

case
    when {{ resource_event_action_type_id }}::number = 1
    then 'added'
    when {{ resource_event_action_type_id }}::number = 2
    then 'removed'
end

{%- endmacro -%}
