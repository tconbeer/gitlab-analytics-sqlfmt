
with
    source as (

        select * from {{ ref("gitlab_dotcom_project_auto_devops_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as project_auto_devops_id,
            project_id::number as project_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            enabled::boolean as has_auto_devops_enabled

        from source
    )

select *
from renamed
