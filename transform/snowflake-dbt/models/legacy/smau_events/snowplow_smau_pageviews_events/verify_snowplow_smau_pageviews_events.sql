{{ config({"materialized": "incremental", "unique_key": "event_surrogate_key"}) }}

{%- set event_ctes = [
    {
        "event_name": "gitlab_ci_yaml_edited",
        "regexp_where_statements": [
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){2,}\/edit\/(([0-9A-Za-z_.-])*){0,}\/.gitlab-ci.yml",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "gitlab_ci_yaml_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){2,}\/blob\/(([0-9A-Za-z_.-])*){0,}\/.gitlab-ci.yml",
                "regexp_function": "REGEXP",
            },
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){2,}\/edit\/(.)*",
                "regexp_function": "NOT REGEXP",
            },
        ],
    },
    {
        "event_name": "job_list_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){2,}\/-\/jobs",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "job_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){2,}\/-\/jobs\/[0-9]{1,}",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "pipeline_charts_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){2,}\/pipelines\/charts",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "pipeline_list_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){2,}\/pipelines",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "pipeline_schedules_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){2,}\/pipeline_schedules",
                "regexp_function": "REGEXP",
            }
        ],
    },
    {
        "event_name": "pipeline_viewed",
        "regexp_where_statements": [
            {
                "regexp_pattern": "(\/([0-9A-Za-z_.-])*){2,}\/pipelines\/[0-9]{1,}",
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
            page_view_start >= '2019-01-01'
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
