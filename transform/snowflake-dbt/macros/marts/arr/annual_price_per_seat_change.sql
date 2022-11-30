{%- macro annual_price_per_seat_change(
    quantity, previous_quantity, arr, previous_arr
) -%}

zeroifnull(
    ({{ arr }} / nullif({{ quantity }}, 0))
    - ({{ previous_arr }} / nullif({{ previous_quantity }}, 0))
) as annual_price_per_seat_change

{%- endmacro -%}
