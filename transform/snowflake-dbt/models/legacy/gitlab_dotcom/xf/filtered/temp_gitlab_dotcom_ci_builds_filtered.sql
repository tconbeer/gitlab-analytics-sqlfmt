{{ config(materialized="incremental") }}

select ci_build_id, ci_build_user_id, created_at, ci_build_project_id
from {{ ref("gitlab_dotcom_ci_builds") }}
where
    created_at is not null and created_at >= dateadd(month, -25, current_date)

    {% if is_incremental() %}

    and created_at > (select max(created_at) from {{ this }})

    {% endif %}
