with
    source as (select * from {{ source("marketo", "activity_update_opportunity") }}),
    renamed as (

        select

            id::number as marketo_activity_update_opportunity_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            data_value_changes::text as data_value_changes

        from source

    )

select *
from renamed
