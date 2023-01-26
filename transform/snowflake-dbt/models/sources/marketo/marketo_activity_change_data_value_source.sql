with
    source as (select * from {{ source("marketo", "activity_change_data_value") }}),
    renamed as (

        select

            id::number as marketo_activity_change_data_value_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            old_value::text as old_value,
            new_value::text as new_value,
            reason::text as reason,
            source::text as source,
            api_method_name::text as api_method_name,
            modifying_user::text as modifying_user,
            request_id::text as request_id

        from source

    )

select *
from renamed
