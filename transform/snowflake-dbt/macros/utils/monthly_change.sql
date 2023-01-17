{%- macro monthly_change(column) -%}

case
    when
        {{ column }} - lag({{ column }}) over (partition by uuid order by created_at)
        >= 0
    then {{ column }} - lag({{ column }}) over (partition by uuid order by created_at)
    else {{ column }}
end as {{ column }}_change

{%- endmacro -%}
