{{
    config(
        {
            "schema": "sensitive",
            "database": env_var("SNOWFLAKE_PREP_DATABASE"),
        }
    )
}}

with
    zendesk_tickets as (select * from {{ ref("zendesk_tickets_source") }}),
    custom_fields as (

        select
            d.value['id'] as ticket_custom_field_id,
            d.value['value'] as ticket_custom_field_value,
            ticket_id as ticket_id
        from
            zendesk_tickets,
            lateral flatten(input => ticket_custom_field_values, outer => true) d

    )

select *
from custom_fields
