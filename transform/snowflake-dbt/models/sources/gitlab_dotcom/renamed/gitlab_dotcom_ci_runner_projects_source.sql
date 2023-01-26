with
    source as (

        select * from {{ ref("gitlab_dotcom_ci_runner_projects_dedupe_source") }}

    ),
    renamed as (

        select

            id::number as ci_runner_project_id,
            runner_id::number as runner_id,
            project_id::number as project_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source
        where project_id is not null

    )

select *
from renamed
