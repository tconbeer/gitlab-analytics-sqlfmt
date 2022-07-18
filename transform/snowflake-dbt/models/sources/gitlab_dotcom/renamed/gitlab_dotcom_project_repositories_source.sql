with
    source as (

        select * from {{ ref("gitlab_dotcom_project_repositories_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as project_repository_id,
            shard_id::number as shard_id,
            disk_path::varchar as disk_path,
            project_id::number as project_id
        from source

    )


select *
from renamed
