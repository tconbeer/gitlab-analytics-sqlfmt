{{ config({"unique_key": "event_surrogate_key"}) }}

{%- set event_ctes = ["mr_created", "mr_comment_added", "snippet_comment_added"] -%}

with
    mr_comment_added as (

        select
            note_author_id as user_id,
            to_date(created_at) as event_date,
            'mr_comment_added' as event_type,
            {{ dbt_utils.surrogate_key(["event_date", "event_type", "note_id"]) }}
            as event_surrogate_key

        from {{ ref("gitlab_dotcom_notes") }}
        where noteable_type = 'MergeRequest' and created_at >= '2015-01-01'

    )

    ,
    mr_created as (

        select
            author_id as user_id,
            to_date(created_at) as event_date,
            'mr_created' as event_type,
            {{
                dbt_utils.surrogate_key(
                    ["event_date", "event_type", "merge_request_id"]
                )
            }} as event_surrogate_key

        from {{ ref("gitlab_dotcom_merge_requests_xf") }}
        where created_at >= '2015-01-01'

    )

    ,
    snippet_comment_added as (

        select
            note_author_id as user_id,
            to_date(created_at) as event_date,
            'snippet_comment_added' as event_type,
            {{ dbt_utils.surrogate_key(["event_date", "event_type", "note_id"]) }}
            as event_surrogate_key

        from {{ ref("gitlab_dotcom_notes") }}
        where noteable_type = 'Snippet' and created_at >= '2015-01-01'

    )

    ,
    unioned as (
        {% for event_cte in event_ctes %}

        select * from {{ event_cte }} {%- if not loop.last %} UNION {%- endif %}

        {% endfor -%}

    )

select *
from unioned
