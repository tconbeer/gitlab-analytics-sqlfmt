with
    source as (select * from {{ source("marketo", "activity_fill_out_form") }}),
    renamed as (

        select

            id::number as marketo_activity_fill_out_form_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            form_fields::text as form_fields,
            client_ip_address::text as client_ip_address,
            webpage_id::number as webpage_id,
            user_agent::text as user_agent,
            query_parameters::text as query_parameters,
            referrer_url::text as referrer_url

        from source

    )

select *
from renamed
