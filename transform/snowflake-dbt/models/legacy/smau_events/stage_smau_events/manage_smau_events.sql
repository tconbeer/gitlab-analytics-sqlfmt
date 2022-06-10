{{ config(tags=["mnpi_exception"]) }}

with
    manage_snowplow_smau_pageviews_events as (

        select
            user_snowplow_domain_id,
            user_custom_id::number as gitlab_user_id,
            event_date,
            event_type,
            event_surrogate_key,
            'snowplow_pageviews' as source_type

        from {{ ref("manage_snowplow_smau_pageviews_events") }}

    )

    ,
    manage_gitlab_dotcom_smau_events as (

        select
            null as user_snowplow_domain_id,
            user_id::number as gitlab_user_id,
            event_date,
            event_type,
            event_surrogate_key,
            'gitlab_backend' as source_type

        from {{ ref("manage_gitlab_dotcom_smau_events") }}

    )

    ,
    unioned as (

        select *
        from manage_snowplow_smau_pageviews_events

        UNION

        select *
        from manage_gitlab_dotcom_smau_events

    )

select *
from unioned
