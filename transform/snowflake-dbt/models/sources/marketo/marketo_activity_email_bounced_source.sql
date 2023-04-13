with
    source as (select * from {{ source("marketo", "activity_email_bounced") }}),
    renamed as (

        select

            id::number as marketo_activity_email_bounced_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            campaign_run_id::number as campaign_run_id,
            category::number as category,
            email::text as email,
            details::text as details,
            subcategory::number as subcategory,
            step_id::number as step_id,
            choice_number::number as choice_number,
            test_variant::number as test_variant

        from source

    )

select *
from renamed
