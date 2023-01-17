with
    source as (select * from {{ source("marketo", "activity_push_lead_to_marketo") }}),
    renamed as (

        select

            id::number as marketo_activity_push_lead_to_marketo_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            api_method_name::text as api_method_name,
            modifying_user::text as modifying_user,
            request_id::text as request_id,
            source::text as source

        from source

    )

select *
from renamed
