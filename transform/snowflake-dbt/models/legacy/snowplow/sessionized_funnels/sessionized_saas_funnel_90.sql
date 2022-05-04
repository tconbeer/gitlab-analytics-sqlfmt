{{ config({"materialized": "view"}) }}

with
    snowplow_page_views_90 as (select * from {{ ref("snowplow_page_views_90") }}),

    snowplow_sessions_90 as (select * from {{ ref("snowplow_sessions_90") }}),

    saas_funnel_subscription_start_page as (

        select
            to_date(min_tstamp) as page_view_date,
            page_view_in_session_index,
            snowplow_page_views_90.session_id,
            page_url_path,
            page_url_query,
            snowplow_page_views_90.referer_url_path,
            min_tstamp
        from snowplow_page_views_90
        left join
            snowplow_sessions_90
            on snowplow_page_views_90.session_id = snowplow_sessions_90.session_id
        where
            snowplow_page_views_90.referer_url_host = 'about.gitlab.com'
            and page_url_path = '/subscriptions/new'
            and rlike (
                page_url_query,
                '(.)*plan_id=(2c92a0ff5a840412015aa3cde86f2ba6|2c92a0fd5a840403015aa6d9ea2c46d6|2c92a0fc5a83f01d015aa6db83c45aac)(.)*'
            )

    )

    ,
    saas_funnel_subscription_success_page as (

        select *
        from snowplow_page_views_90
        where
            (
                rlike (
                    snowplow_page_views_90.page_url_path,
                    '/subscriptions/([a-zA-Z0-9\-]{1,})/success/create_subscription'
                -- SaaS packages have these 3 plan_id
                ) and rlike (
                    snowplow_page_views_90.referer_url_query,
                    '(.)*plan_id=(2c92a0ff5a840412015aa3cde86f2ba6|2c92a0fd5a840403015aa6d9ea2c46d6|2c92a0fc5a83f01d015aa6db83c45aac)(.)*'
                )
            )
    )

    ,
    joined as (

        select
            saas_funnel_subscription_start_page.session_id,
            saas_funnel_subscription_start_page.session_id
            is
            not null
            as subscription_funnel_start_page,
            min(
                saas_funnel_subscription_start_page.min_tstamp
            ) as subscription_funnel_start_min_tsamp,
            saas_funnel_subscription_success_page.session_id
            is
            not null
            as subscription_funnel_success_page,
            min(
                saas_funnel_subscription_success_page.min_tstamp
            ) as subscription_funnel_success_min_tsamp
        from saas_funnel_subscription_start_page
        left join
            saas_funnel_subscription_success_page
            on saas_funnel_subscription_start_page.session_id
            = saas_funnel_subscription_success_page.session_id
        group by 1, 2, 4
    )

select *
from joined
order by subscription_funnel_start_page asc
