with
    source as (select * from {{ source("marketo", "activity_sfdc_activity") }}),
    renamed as (

        select

            id::number as marketo_activity_sfdc_activity_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            status::text as status,
            description::text as description,
            is_task::boolean as is_task,
            priority::text as priority,
            activity_owner::text as activity_owner,
            due_date::date as due_date

        from source

    )

select *
from renamed
