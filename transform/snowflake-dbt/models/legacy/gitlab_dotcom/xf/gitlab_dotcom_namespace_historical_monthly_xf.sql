with
    date_details as (

        select * from {{ ref("date_details") }} where last_day_of_month = date_actual

    ),
    namespace as (select * from {{ ref("gitlab_dotcom_namespace_historical_daily") }}),
    lineage as (

        select * from {{ ref("gitlab_dotcom_namespace_lineage_historical_daily") }}

    ),
    statistics as (

        select * from {{ ref("gitlab_dotcom_namespace_statistics_historical_monthly") }}

    ),
    storage as (

        select *
        from {{ ref("gitlab_dotcom_namespace_storage_statistics_historical_monthly") }}

    )

select
    date_details.date_actual as snapshot_month,
    namespace.*,
    lineage.ultimate_parent_id,
    lineage.ultimate_parent_plan_id,
    lineage.namespace_is_internal,
    statistics.shared_runners_seconds,
    statistics.shared_runners_seconds_last_reset,
    storage.repository_size,
    storage.lfs_objects_size,
    storage.wiki_size,
    storage.build_artifacts_size,
    storage.storage_size,
    storage.packages_size
from namespace
left join
    lineage
    on namespace.namespace_id = lineage.namespace_id
    and namespace.snapshot_day = lineage.snapshot_day
left join
    statistics
    on namespace.namespace_id = statistics.namespace_id
    and namespace.snapshot_day = statistics.snapshot_month
left join
    storage
    on namespace.namespace_id = storage.namespace_id
    and namespace.snapshot_day = storage.snapshot_month
inner join date_details on date_details.date_actual = namespace.snapshot_day
