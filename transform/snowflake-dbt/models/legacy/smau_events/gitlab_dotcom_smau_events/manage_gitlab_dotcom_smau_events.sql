{{ config(tags=["mnpi_exception"]) }}

{{ config({"unique_key": "event_surrogate_key"}) }}

{%- set event_ctes = ["project_created", "user_created"] -%}

with
    project_created as (

        select
            creator_id as user_id,
            to_date(project_created_at) as event_date,
            'project_created' as event_type,
            {{ dbt_utils.surrogate_key(["event_date", "event_type", "project_id"]) }}
            as event_surrogate_key

        from {{ ref("gitlab_dotcom_projects_xf") }}
        where project_created_at >= '2015-01-01'

    )

    ,
    user_created as (

        select
            user_id,
            to_date(created_at) as event_date,
            'user_created' as event_type,
            {{ dbt_utils.surrogate_key(["event_date", "event_type", "user_id"]) }}
            as event_surrogate_key

        from {{ ref("gitlab_dotcom_users_xf") }}
        where created_at >= '2015-01-01'

    )

    ,
    unioned as (
        {% for event_cte in event_ctes %}

        select * from {{ event_cte }} {%- if not loop.last %} UNION {%- endif %}

        {% endfor -%}

    )

select *
from unioned
