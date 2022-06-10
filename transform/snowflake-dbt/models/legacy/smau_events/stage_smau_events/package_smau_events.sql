with
    package_snowplow_smau_pageviews_events as (

        select
            user_snowplow_domain_id,
            user_custom_id as gitlab_user_id,
            event_date,
            event_type,
            event_surrogate_key as event_surrogate_key,
            'snowplow_pageviews' as source_type

        from {{ ref("package_snowplow_smau_pageviews_events") }}

    )

    ,
    package_snowplow_smau_structured_events as (

        select
            user_snowplow_domain_id,
            user_custom_id as gitlab_user_id,
            event_date,
            event_type,
            event_surrogate_key as event_surrogate_key,
            'snowplow_structured_events' as source_type

        from {{ ref("package_snowplow_smau_structured_events") }}

    )

    ,
    unioned as (

        select *
        from package_snowplow_smau_pageviews_events

        UNION

        select *
        from package_snowplow_smau_structured_events

    )

select *
from unioned
