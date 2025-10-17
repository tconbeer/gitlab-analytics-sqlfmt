with
    plan_snowplow_smau_pageviews_events as (

        select
            user_snowplow_domain_id,
            user_custom_id as gitlab_user_id,
            event_date,
            event_type,
            event_surrogate_key as event_surrogate_key,
            'snowplow_pageviews' as source_type

        from {{ ref("plan_snowplow_smau_pageviews_events") }}

    )

select *
from plan_snowplow_smau_pageviews_events
