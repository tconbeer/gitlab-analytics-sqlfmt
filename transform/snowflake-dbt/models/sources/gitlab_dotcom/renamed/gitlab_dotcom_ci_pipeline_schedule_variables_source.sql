with
    source as (

        select *
        from {{ ref("gitlab_dotcom_ci_pipeline_schedule_variables_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as ci_pipeline_schedule_variable_id,
            key as key,
            pipeline_schedule_id::number as ci_pipeline_schedule_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            variable_type as variable_type

        from source

    )


select *
from renamed
