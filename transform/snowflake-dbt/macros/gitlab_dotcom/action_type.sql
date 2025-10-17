{%- macro action_type(action_type_id) -%}

    case
        when {{ action_type_id }}::number = 1
        then 'created'
        when {{ action_type_id }}::number = 2
        then 'updated'
        when {{ action_type_id }}::number = 3
        then 'closed'
        when {{ action_type_id }}::number = 4
        then 'reopened'
        when {{ action_type_id }}::number = 5
        then 'pushed'
        when {{ action_type_id }}::number = 6
        then 'commented'
        when {{ action_type_id }}::number = 7
        then 'merged'
        when {{ action_type_id }}::number = 8
        then 'joined'
        when {{ action_type_id }}::number = 9
        then 'left'
        when {{ action_type_id }}::number = 10
        then 'destroyed'
        when {{ action_type_id }}::number = 11
        then 'expired'
    end

{%- endmacro -%}
