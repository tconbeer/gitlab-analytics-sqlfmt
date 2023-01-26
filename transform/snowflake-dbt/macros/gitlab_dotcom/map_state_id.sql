{%- macro map_state_id(state_id) -%}

case
    when {{ state_id }}::number = 1
    then 'opened'
    when {{ state_id }}::number = 2
    then 'closed'
    when {{ state_id }}::number = 3
    then 'merged'
    when {{ state_id }}::number = 4
    then 'locked'
    else null
end

{%- endmacro -%}
