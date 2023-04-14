{{
    config(
        {
            "materialized": "incremental",
            "unique_key": "id",
            "alias": "gitlab_dotcom_ci_builds_dedupe_source",
            "post-hook": '{{ apply_dynamic_data_masking(columns = [{"id":"number"},{"commit_id":"number"},{"name":"string"},{"options":"string"},{"ref":"string"},{"user_id":"number"},{"project_id":"number"},{"erased_by_id":"number"},{"environment":"string"},{"yaml_variables":"string"},{"auto_canceled_by_id":"number"}]) }}',
        }
    )
}}

select *
from {{ source("gitlab_dotcom", "ci_builds") }}
{% if is_incremental() %}

    where updated_at >= (select max(updated_at) from {{ this }})

{% endif %}
qualify row_number() over (partition by id order by updated_at desc) = 1
