with
    source as (

        select * from {{ ref("gitlab_dotcom_ci_sources_pipelines_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as ci_source_pipeline_id,
            project_id::number as project_id,
            pipeline_id::number as pipeline_id,
            source_project_id::number as source_project_id,
            source_pipeline_id::number as source_pipeline_id,
            source_job_id::number as source_job_id
        from source

    )

select *
from renamed
