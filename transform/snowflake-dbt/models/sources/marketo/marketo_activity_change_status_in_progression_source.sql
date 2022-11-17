with
    source as (

        select * from {{ source("marketo", "activity_change_status_in_progression") }}

    ),
    renamed as (

        select

            id::number as marketo_activity_change_status_in_progression_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            old_status_id::number as old_status_id,
            new_status_id::number as new_status_id,
            acquired_by::boolean as acquired_by,
            old_status::text as old_status,
            new_status::text as new_status,
            program_member_id::number as program_member_id,
            success::boolean as success,
            registration_code::text as registration_code,
            webinar_url::text as webinar_url,
            reason::text as reason,
            reached_success_date::timestamp_ntz as reached_success_date

        from source

    )

select *
from renamed
