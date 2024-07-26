with
    source as (select * from {{ source("marketo", "activity_visit_webpage") }}),
    renamed as (

        select

            id::number as marketo_activity_visit_webpage_id,
            lead_id::number as lead_id,
            activity_date::timestamp_tz as activity_date,
            activity_type_id::number as activity_type_id,
            campaign_id::number as campaign_id,
            primary_attribute_value_id::number as primary_attribute_value_id,
            primary_attribute_value::text as primary_attribute_value,
            webpage_url::text as webpage_url,
            search_engine::text as search_engine,
            client_ip_address::text as client_ip_address,
            user_agent::text as user_agent,
            query_parameters::text as query_parameters,
            referrer_url::text as referrer_url,
            search_query::text as search_query

        from source

    )

select *
from renamed
