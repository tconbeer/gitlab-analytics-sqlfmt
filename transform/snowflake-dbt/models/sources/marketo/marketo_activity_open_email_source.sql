with
    source as (select * from {{ source("marketo", "activity_open_email") }}),
    renamed as (

        select

            id::number as marketo_activity_open_email_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            campaign_run_id::number as campaign_run_id,
            platform::text as platform,
            is_mobile_device::boolean as is_mobile_device,
            step_id::number as step_id,
            device::text as device,
            test_variant::number as test_variant,
            choice_number::number as choice_number,
            is_bot_activity::boolean as is_bot_activity,
            user_agent::text as user_agent,
            bot_activity_pattern::text as bot_activity_pattern

        from source

    )

select *
from renamed
