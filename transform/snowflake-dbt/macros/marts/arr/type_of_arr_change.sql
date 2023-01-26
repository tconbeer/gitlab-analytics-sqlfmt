{%- macro type_of_arr_change(arr, previous_arr, row_number) -%}

case
    when {{ row_number }} = 1
    then 'New'
    when {{ arr }} = 0 and {{ previous_arr }} > 0
    then 'Churn'
    when {{ arr }} < {{ previous_arr }} and {{ arr }} > 0
    then 'Contraction'
    when {{ arr }} > {{ previous_arr }} and {{ row_number }} > 1
    then 'Expansion'
    when {{ arr }} = {{ previous_arr }}
    then 'No Impact'
    else null
end as type_of_arr_change

{%- endmacro -%}
