with
    source as (

        select * from {{ source("marketo", "activity_change_nurture_cadence") }}

    ),
    renamed as (

        select

            id::number as marketo_activity_change_nurture_cadence_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            new_nurture_cadence::text as new_nurture_cadence,
            previous_nurture_cadence::text as previous_nurture_cadence

        from source

    )

select *
from renamed
