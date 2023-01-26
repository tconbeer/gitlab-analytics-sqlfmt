{%- macro monthly_price_per_seat_change(
    original_mrr,
    original_seat_quantity,
    original_unit_of_measure,
    new_mrr,
    new_seat_quantity,
    new_unit_of_measure
) -%}

case
    when
        -- Only compare prices per seat when the unit of measure of the original and
        -- new plans is seats
        not (
            {{ original_unit_of_measure }} = array_construct('Seats')
            and {{ new_unit_of_measure }} = array_construct('Seats')
        )
    then null
    else
        ({{ new_mrr }} / {{ new_seat_quantity }})
        - ({{ original_mrr }} / {{ original_seat_quantity }})
end as monthly_price_per_seat_change

{%- endmacro -%}
