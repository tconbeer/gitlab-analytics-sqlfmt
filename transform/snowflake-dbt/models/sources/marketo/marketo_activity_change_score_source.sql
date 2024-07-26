with
    source as (select * from {{ source("marketo", "activity_change_score") }}),
    renamed as (

        select

            id::number as marketo_activity_change_score_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            change_value::text as change_value,
            old_value::number as old_value,
            new_value::number as new_value,
            reason::text as reason,
            relative_urgency::number as relative_urgency,
            priority::number as priority,
            relative_score::number as relative_score,
            urgency::float as urgency

        from source

    )

select *
from renamed
