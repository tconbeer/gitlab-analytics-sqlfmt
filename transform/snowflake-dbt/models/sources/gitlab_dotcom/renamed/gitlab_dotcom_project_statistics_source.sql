with
    source as (

        select * from {{ ref("gitlab_dotcom_project_statistics_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as project_statistics_id,
            project_id::number as project_id,
            namespace_id::number as namespace_id,
            commit_count::number as commit_count,
            storage_size::number as storage_size,
            repository_size::number as repository_size,
            lfs_objects_size::number as lfs_objects_size,
            build_artifacts_size::number as build_artifacts_size,
            packages_size::number as packages_size,
            wiki_size::number as wiki_size,
            shared_runners_seconds::number as shared_runners_seconds,
            shared_runners_seconds_last_reset::timestamp as last_update_started_at
        from source

    )

select *
from renamed
