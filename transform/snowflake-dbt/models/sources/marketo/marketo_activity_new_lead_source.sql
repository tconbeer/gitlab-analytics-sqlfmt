with
    source as (select * from {{ source("marketo", "activity_new_lead") }}),
    renamed as (

        select

            id::number as marketo_activity_new_lead_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            modifying_user::text as modifying_user,
            created_date::date as created_date,
            api_method_name::text as api_method_name,
            source_type::text as source_type,
            request_id::text as request_id,
            form_name::text as form_name,
            lead_source::text as lead_source,
            sfdc_type::text as sfdc_type,
            list_name::text as list_name

        from source

    )

select *
from renamed
