with
    source as (select * from {{ ref("gitlab_dotcom_ci_job_artifacts_dedupe_source") }}),
    renamed as (

        select
            id::number as ci_job_artifact_id,
            project_id::number as project_id,
            job_id::number as ci_job_id,
            file_type as file_type,
            size as size,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            expire_at::timestamp as expire_at,
            file as file,
            file_store as file_store,
            file_format as file_format,
            file_location as file_location,
            locked as locked
        from source

    )

select *
from renamed
