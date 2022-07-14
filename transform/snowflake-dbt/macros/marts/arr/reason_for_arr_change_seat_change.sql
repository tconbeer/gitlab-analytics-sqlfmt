{%- macro reason_for_arr_change_seat_change(
    quantity, previous_quantity, arr, previous_arr
) -%}

case
    when {{ previous_quantity }} != {{ quantity }} and {{ previous_quantity }} > 0
    then
        zeroifnull(
            {{ previous_arr }} / nullif(
                {{ previous_quantity }}, 0
            ) * (
                {{ quantity }} - {{ previous_quantity }}
            )
        )
    when {{ previous_quantity }} != {{ quantity }} and {{ previous_quantity }} = 0
    then {{ arr }}
    else 0
end as seat_change_arr

{%- endmacro -%}
