with
    source as (

        select * from {{ ref("gitlab_dotcom_lfs_objects_projects_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as lfs_object_project_id,
            lfs_object_id::number as lfs_object_id,
            project_id::number as project_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            repository_type::varchar as repository_type

        from source

    )

select *
from renamed
