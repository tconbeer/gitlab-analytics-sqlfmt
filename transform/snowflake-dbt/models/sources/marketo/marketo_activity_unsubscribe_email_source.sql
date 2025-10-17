with
    source as (select * from {{ source("marketo", "activity_unsubscribe_email") }}),
    renamed as (

        select

            id::number as marketo_activity_unsubscribe_email_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            campaign_run_id::number as campaign_run_id,
            webform_id::number as webform_id,
            client_ip_address::text as client_ip_address,
            form_fields::text as form_fields,
            webpage_id::number as webpage_id,
            user_agent::text as user_agent,
            query_parameters::text as query_parameters,
            referrer_url::text as referrer_url,
            test_variant::number as test_variant

        from source

    )

select *
from renamed
