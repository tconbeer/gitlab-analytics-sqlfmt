with
    source as (

        select * from {{ ref("gitlab_dotcom_experiment_subjects_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as experiment_subject_id,
            experiment_id::number as experiment_id,
            user_id::number as user_id,
            group_id::number as group_id,
            project_id::number as project_id,
            variant::number as experiment_variant,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            converted_at::timestamp as converted_at
        from source

    )

select *
from renamed
