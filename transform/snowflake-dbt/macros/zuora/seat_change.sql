{%- macro seat_change(
    original_seat_quantity,
    original_unit_of_measure,
    original_mrr,
    new_seat_quantity,
    new_unit_of_measure,
    new_mrr
) -%}

case
    when {{ new_mrr }} = 0 and {{ original_mrr }} > 0
    then 'Cancelled'
    -- Only compare prices per seat when the unit of measure of the original and new
    -- plans is seats
    when
        not (
            {{ original_unit_of_measure }} = array_construct('Seats')
            and {{ new_unit_of_measure }} = array_construct('Seats')
        )
    then 'Not Valid'
    when {{ original_seat_quantity }} = {{ new_seat_quantity }}
    then 'Maintained'
    when {{ original_seat_quantity }} > {{ new_seat_quantity }}
    then 'Contraction'
    when {{ original_seat_quantity }} < {{ new_seat_quantity }}
    then 'Expansion'
end as seat_change

{%- endmacro -%}
