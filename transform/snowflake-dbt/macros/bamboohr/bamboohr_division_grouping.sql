{%- macro bamboohr_division_grouping(division) -%}

case
    when {{ division }} in ('Engineering', 'Meltano')
    then 'Engineering/Meltano'
    when {{ division }} in ('CEO', 'People Group')
    then 'People Group/CEO'
    else {{ division }}
end

{%- endmacro -%}
