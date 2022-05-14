with
    create_snowplow_smau_pageviews_events as (

        select
            user_snowplow_domain_id,
            user_custom_id as gitlab_user_id,
            event_date,
            event_type,
            event_surrogate_key as event_surrogate_key,
            'snowplow_pageviews' as source_type

        from {{ ref("create_snowplow_smau_pageviews_events") }}

    )

    ,
    create_gitlab_dotcom_smau_events as (

        select
            null as user_snowplow_domain_id,
            user_id as gitlab_user_id,
            event_date,
            event_type,
            event_surrogate_key,
            'gitlab_backend' as source_type

        from {{ ref("create_gitlab_dotcom_smau_events") }}

    )

    ,
    unioned as (

        select *
        from create_snowplow_smau_pageviews_events

        union

        select *
        from create_gitlab_dotcom_smau_events

    )

select *
from unioned
