{{ config(materialized="incremental", cluster_by=["noteable_type"]) }}

select note_author_id, project_id, note_id, created_at, noteable_type
from {{ ref("gitlab_dotcom_notes") }}
where
    created_at is not null
    and created_at >= dateadd(month, -25, current_date)
    and noteable_type in ('Issue', 'MergeRequest')

    {% if is_incremental() %}

        and created_at > (select max(created_at) from {{ this }})

    {% endif %}
