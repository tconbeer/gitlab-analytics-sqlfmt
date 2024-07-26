{{
    config(
        {
            "alias": "gitlab_dotcom_ci_job_artifacts",
            "post-hook": '{{ apply_dynamic_data_masking(columns = [{"ci_job_artifact_id":"number"},{"project_id":"number"},{"ci_job_id":"number"},{"file":"string"} ]) }}',
        }
    )
}}

with source as (select * from {{ ref("gitlab_dotcom_ci_job_artifacts_source") }})

select *
from source
