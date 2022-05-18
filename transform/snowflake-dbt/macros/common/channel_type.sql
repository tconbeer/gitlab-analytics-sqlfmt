{%- macro channel_type(sqs_bucket_engagement, order_type) -%}

case
    when
        {{ sqs_bucket_engagement }} = 'Partner Sourced'
        and {{ order_type }} = '1. New - First Order'
    then 'Sourced - New'
    when
        {{ sqs_bucket_engagement }} = 'Partner Sourced' and (
            {{ order_type }} != '1. New - First Order' or {{ order_type }} is null
        )
    then 'Sourced - Growth'
    when
        {{ sqs_bucket_engagement }} = 'Co-sell'
        and {{ order_type }} = '1. New - First Order'
    then 'Co-sell - New'
    when
        {{ sqs_bucket_engagement }} = 'Co-sell' and (
            {{ order_type }} != '1. New - First Order' or {{ order_type }} is null
        )
    then 'Co-sell - Growth'
end

{%- endmacro -%}
