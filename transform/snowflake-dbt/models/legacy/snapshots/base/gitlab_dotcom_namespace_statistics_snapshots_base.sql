{{ config({"alias": "gitlab_dotcom_namespace_statistics_snapshots"}) }}

with
    source as (

        select *
        from {{ source("snapshots", "gitlab_dotcom_namespace_statistics_snapshots") }}

    ),
    renamed as (

        select

            dbt_scd_id::varchar as namespace_statistics_snapshot_id,
            id::number as namespace_statistics_id,
            namespace_id::number as namespace_id,
            shared_runners_seconds::number as shared_runners_seconds,
            shared_runners_seconds_last_reset::timestamp
            as shared_runners_seconds_last_reset,
            dbt_valid_from::timestamp as valid_from,
            dbt_valid_to::timestamp as valid_to

        from source

    )

select *
from renamed
