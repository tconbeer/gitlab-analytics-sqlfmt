{%- macro reason_for_quantity_change_seat_change(quantity, previous_quantity) -%}

    case
        when {{ previous_quantity }} != {{ quantity }}
        then {{ quantity }} - {{ previous_quantity }}
        else 0
    end as seat_change_quantity

{%- endmacro -%}
