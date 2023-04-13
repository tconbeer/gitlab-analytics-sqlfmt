{{ config({"materialized": "incremental", "unique_key": "event_surrogate_key"}) }}

{%- set event_ctes = [
    {
        "event_name": "alert_management_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "((\/([0-9A-Za-z_.-])*){2,})?\/alert_management",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "alert_management_detail_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "((\/([0-9A-Za-z_.-])*){2,})?\/alert_management\/(.)*",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "envrionments_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "((\/([0-9A-Za-z_.-])*){2,})?\/environments$",
                "regexp_function": "REGEXP",
            },
            {
                "regexp_pattern": "/help/ci/environments",
                "regexp_function": "NOT REGEXP",
            },
        ],
    },
    {
        "event_name": "error_tracking_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "((\/([0-9A-Za-z_.-])*){2,})?\/error_tracking",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "logging_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "((\/([0-9A-Za-z_.-])*){2,})?\/logs",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "metrics_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "((\/([0-9A-Za-z_.-])*){2,})?\/metrics",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "operations_settings_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "((\/([0-9A-Za-z_.-])*){2,})?\/settings\/operations",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "prometheus_edited",
        "regexp_where_statements": [
            {
                "regexp_pattern": "((\/([0-9A-Za-z_.-])*){2,})?\/services\/prometheus\/edit",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "tracing_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "((\/([0-9A-Za-z_.-])*){2,})?\/tracing",
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
