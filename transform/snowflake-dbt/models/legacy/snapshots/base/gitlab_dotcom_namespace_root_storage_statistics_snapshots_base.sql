{{ config({"alias": "gitlab_dotcom_namespace_root_storage_statistics_snapshots"}) }}

with
    source as (

        select *
        from
            {{
                source(
                    "snapshots",
                    "gitlab_dotcom_namespace_root_storage_statistics_snapshots",
                )
            }}

    ),
    renamed as (

        select
            dbt_scd_id::varchar as namespace_storage_statistics_snapshot_id,
            namespace_id::number as namespace_id,
            repository_size::number as repository_size,
            lfs_objects_size::number as lfs_objects_size,
            wiki_size::number as wiki_size,
            build_artifacts_size::number as build_artifacts_size,
            storage_size::number as storage_size,
            packages_size::number as packages_size,
            dbt_valid_from::timestamp as valid_from,
            dbt_valid_to::timestamp as valid_to

        from source

    )

select *
from renamed
