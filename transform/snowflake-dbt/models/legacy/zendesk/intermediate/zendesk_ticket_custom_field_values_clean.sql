{{ config({"schema": "legacy"}) }}

with
    zendesk_custom_fields as (

        select * from {{ ref("zendesk_ticket_custom_field_values_sensitive") }}

    ),
    filtered as (

        -- Transactions Issue Type
        select * from zendesk_custom_fields where ticket_custom_field_id = 360020421853

    )

select *
from filtered
