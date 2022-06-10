{%- macro retention_reason(
    original_mrr,
    original_product_category,
    original_product_ranking,
    original_seat_quantity,
    new_mrr,
    new_product_category,
    new_product_ranking,
    new_seat_quantity
) -%}

case
    when
        {{ original_mrr }} > {{ new_mrr }}
        and {{ original_product_category }} = {{ new_product_category }}
        and {{ original_seat_quantity }} > {{ new_seat_quantity }}
        and
        {{ original_mrr }}
        / {{ original_seat_quantity }}
        > {{ new_mrr }}
        / {{ new_seat_quantity }}
    then 'Price Change/Seat Change Mix'

    when
        {{ original_mrr }} < {{ new_mrr }}
        and {{ original_product_category }} = {{ new_product_category }}
        and {{ original_seat_quantity }} < {{ new_seat_quantity }}
        and
        {{ original_mrr }}
        / {{ original_seat_quantity }}
        < {{ new_mrr }}
        / {{ new_seat_quantity }}
    then 'Price Change/Seat Change Mix'

    when
        {{ original_mrr }} > {{ new_mrr }}
        and {{ original_product_category }} = {{ new_product_category }}
        and {{ original_seat_quantity }} > {{ new_seat_quantity }}
    then 'Seat Change'

    when
        {{ original_mrr }} < {{ new_mrr }}
        and {{ original_product_category }} = {{ new_product_category }}
        and {{ original_seat_quantity }} < {{ new_seat_quantity }}
    then 'Seat Change'

    when
        {{ original_mrr }} > {{ new_mrr }} and (
            {{ original_product_category }} = {{ new_product_category }}
            and {{ original_seat_quantity }} <= {{ new_seat_quantity }}
        )
    then 'Price Change'

    when
        {{ original_mrr }} < {{ new_mrr }} and (
            {{ original_product_category }} = {{ new_product_category }}
            and {{ original_seat_quantity }} >= {{ new_seat_quantity }}
        )
    then 'Price Change'

    when
        {{ original_mrr }} > {{ new_mrr }}
        and {{ original_product_ranking }} < {{ new_product_ranking }}
        and {{ original_seat_quantity }} = {{ new_seat_quantity }}
    then 'Price Change'

    when
        {{ original_mrr }} < {{ new_mrr }}
        and {{ original_product_ranking }} > {{ new_product_ranking }}
        and {{ original_seat_quantity }} = {{ new_seat_quantity }}
    then 'Price Change'

    when
        {{ original_product_category }} != {{ new_product_category }}
        and {{ original_seat_quantity }} = {{ new_seat_quantity }}
    then 'Product Change'

    when
        {{ original_product_category }} != {{ new_product_category }}
        and {{ original_seat_quantity }} != {{ new_seat_quantity }}
    then 'Product Change/Seat Change Mix'

    when {{ new_mrr }} = 0 and {{ original_mrr }} > 0
    then 'Cancelled'

    else 'Unknown'
end as retention_reason

{%- endmacro -%}
