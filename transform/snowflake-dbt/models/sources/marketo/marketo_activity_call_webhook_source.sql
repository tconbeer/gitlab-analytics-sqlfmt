with
    source as (select * from {{ source("marketo", "activity_call_webhook") }}),
    renamed as (

        select

            id::number as marketo_activity_call_webhook_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            response::text as response,
            error_type::number as error_type

        from source
    )

select *
from renamed
