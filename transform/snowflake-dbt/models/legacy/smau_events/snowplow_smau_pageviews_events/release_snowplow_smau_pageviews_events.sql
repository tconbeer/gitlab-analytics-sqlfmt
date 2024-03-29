{{ config({"materialized": "incremental", "unique_key": "event_surrogate_key"}) }}

{%- set event_ctes = [
    {
        "event_name": "environment_page_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "((\/([0-9A-Za-z_.-])*){2,})?\/environments$",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "feature_flags_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "((\/([0-9A-Za-z_.-])*){2,})?\/-\/feature_flags$",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "release_page_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "((\/([0-9A-Za-z_.-])*){2,})?\/-\/releases$",
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
            page_view_id
        from {{ ref("snowplow_page_views_all") }}
        where
            true
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
    unioned as (

        {% for event_cte in event_ctes %}

            select *
            from {{ event_cte.event_name }}

            {%- if not loop.last %}
                union
            {%- endif %}

        {% endfor -%}

    )

select *
from unioned
