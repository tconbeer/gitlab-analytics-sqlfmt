{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "id",
            "alias": "gitlab_dotcom_notes_dedupe_source",
            "post-hook": '{{ apply_dynamic_data_masking(columns = [{"id":"number"},{"note":"string"},{"author_id":"float"},{"project_id":"number"},{"line_code":"string"},{"commit_id":"string"},{"noteable_id":"float"},{"updated_by_id":"float"},{"position":"string"},{"original_position":"string"},{"resolved_by_id":"string"},{"discussion_id":"string"},{"note_html":"string"},{"change_position":"string"}]) }}',
        }
    )
}}


select *
from {{ source("gitlab_dotcom", "notes") }}
{% if is_incremental() %}

where updated_at >= (select max(updated_at) from {{ this }})

{% endif %}
qualify row_number() over (partition by id order by updated_at desc) = 1
