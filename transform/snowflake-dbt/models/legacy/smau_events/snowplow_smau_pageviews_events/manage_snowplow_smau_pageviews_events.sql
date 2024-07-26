{{ config({"materialized": "incremental", "unique_key": "event_surrogate_key"}) }}

{%- set event_ctes = [
    {
        "event_name": "audit_events_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){1,}\/audit_events",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "cycle_analytics_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){2,}\/cycle_analytics",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "insights_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){1,}\/insights",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "group_analytics_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){1,}\/analytics",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "group_created",
        "regexp_where_statements": [
            {"regexp_pattern": "\/groups\/new", "regexp_function": "REGEXP"}
        ],
    },
    {
        "event_name": "productivity_analytics_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){1,}\/productivity_analytics",
                "regexp_function": "REGEXP",
            }
        ],
    },
] -%}

with
    snowplow_page_views as (

        select
            user_snowplow_domain_id,
            user_custom_id,
            page_view_start,
            page_url_path,
            page_view_id,
            referer_url_path
        from {{ ref("snowplow_page_views_all") }}
        where
            true and app_id = 'gitlab'
            {% if is_incremental() %}
                and page_view_start >= (select max(event_date) from {{ this }})
            {% endif %}

    )

    {% for event_cte in event_ctes %}

        ,
        {{
            smau_events_ctes(
                event_name=event_cte.event_name,
                regexp_where_statements=event_cte.regexp_where_statements,
            )
        }}

    {% endfor -%},
    /*
    Looks at referrer_url in addition to page_url.
    Regex matches for successful sign-in authentications,
    meaning /sign_in redirects to a real GitLab page.
  */
    user_authenticated as (

        select
            user_snowplow_domain_id,
            user_custom_id,
            to_date(page_view_start) as event_date,
            page_url_path,
            'user_authenticated' as event_type,
            {{ dbt_utils.surrogate_key(["page_view_id", "event_type"]) }}
            as event_surrogate_key
        from snowplow_page_views
        where
            referer_url_path regexp '\/users\/sign_in'
            and page_url_path not regexp '\/users\/sign_in'

    ),
    unioned as (

        {% for event_cte in event_ctes %}

            select *
            from {{ event_cte.event_name }}

            {%- if not loop.last %}
                union
            {%- endif %}

        {% endfor -%}

        union

        select *
        from user_authenticated

    )

select *
from unioned
