with
    source as (

        select *
        from {{ ref("gitlab_dotcom_namespace_root_storage_statistics_dedupe_source") }}

    ),
    renamed as (

        select

            namespace_id::number as namespace_id,
            repository_size::number as repository_size,
            lfs_objects_size::number as lfs_objects_size,
            wiki_size::number as wiki_size,
            build_artifacts_size::number as build_artifacts_size,
            storage_size::number as storage_size,
            packages_size::number as packages_size,
            updated_at::timestamp as namespace_updated_at

        from source

    )

select *
from renamed
