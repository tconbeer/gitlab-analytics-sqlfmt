-- To convert storage (usage) sizes from bytes in source to GiB for reporting (1 GiB =
-- 2^30 bytes = 1,073,741,824 bytes)
{% set bytes_to_gib_conversion = 1073741824 %}
-- To convert storage (usage) sizes from bytes in source to MiB for reporting (1 MiB =
-- 2^20 bytes = 1,048,576 bytes)
{% set bytes_to_mib_conversion = 1048576 %}
-- To convert storage limit sizes from GiB in "source" to MiB for reporting (1 GiB =
-- 1024 MiB)
{% set mib_to_gib_conversion = 1024 %}

with
    project_statistics_snapshot_monthly_all as (

        -- project_statistics_snapshot_monthly 
        select
            snapshot_month,
            project_id,
            namespace_id,
            (repository_size + lfs_objects_size)
            / {{ bytes_to_gib_conversion }} as project_storage_size
        from {{ ref("gitlab_dotcom_project_statistic_historical_monthly") }}
        where
            snapshot_month >= '2020-07-01'
            and snapshot_month < date_trunc('month', current_date)

        union all

        -- project_statistics_current
        select
            date_trunc('month', current_date) as snapshot_month,
            project_id,
            namespace_id,
            (repository_size + lfs_objects_size)
            / {{ bytes_to_gib_conversion }} as project_storage_size
        from {{ ref("gitlab_dotcom_project_statistics_source") }}

    ),
    namespace_lineage_monthly_all as (

        -- namespace_lineage_monthly
        select snapshot_month, namespace_id, ultimate_parent_id
        from {{ ref("gitlab_dotcom_namespace_lineage_historical_monthly") }}
        where
            snapshot_month >= '2020-07-01'
            and snapshot_month < date_trunc('month', current_date)

        union all

        -- namespace_lineage_current
        select
            date_trunc('month', current_date) as snapshot_month,
            dim_namespace_id,
            ultimate_parent_namespace_id
        from {{ ref("prep_namespace_lineage") }}

    ),
    namespace_storage_statistic_monthly_all as (

        -- namespace_storage_statistic_monthly
        select
            snapshot_month,
            namespace_id,
            storage_size,
            repository_size,
            lfs_objects_size,
            build_artifacts_size,
            packages_size,
            wiki_size,
            repository_size + lfs_objects_size as billable_storage_size
        from {{ ref("gitlab_dotcom_namespace_storage_statistics_historical_monthly") }}
        where
            snapshot_month >= '2020-07-01'
            and snapshot_month < date_trunc('month', current_date)

        union all

        -- namespace_storage_statistic_current
        select
            date_trunc('month', current_date) as snapshot_month,
            namespace_id,
            storage_size,
            repository_size,
            lfs_objects_size,
            build_artifacts_size,
            packages_size,
            wiki_size,
            repository_size + lfs_objects_size as billable_storage_size
        from {{ ref("gitlab_dotcom_namespace_root_storage_statistics_source") }}

    ),
    month_spine as (

        select * from {{ ref("dim_date") }} where date_actual = first_day_of_month

    ),
    purchased_storage as (

        select
            gitlab_namespace_id::int as namespace_id,
            month_spine.first_day_of_month as snapshot_month,
            sum(order_quantity * 10) as purchased_storage_gib
        from {{ ref("customers_db_orders_source") }}
        inner join
            month_spine
            on month_spine.first_day_of_month between date_trunc(
                'month', order_start_date
            )
            and dateadd(month, -1, date_trunc('month', order_end_date))
        -- only storage rate plan, 10GiB of storage
        where product_rate_plan_id = '2c92a00f7279a6f5017279d299d01cf9'
        group by 1, 2

    ),
    top_level_namespace_storage_summary as (

        select
            -- Only top level namespaces
            namespace_lineage_monthly_all.ultimate_parent_id,
            namespace_lineage_monthly_all.snapshot_month,
            sum(
                coalesce(purchased_storage.purchased_storage_gib, 0)
            ) as purchased_storage_limit,
            sum(
                namespace_storage_statistic_monthly_all.billable_storage_size
            ) as billable_storage_size,
            sum(
                namespace_storage_statistic_monthly_all.repository_size
            ) as repository_size,
            sum(
                namespace_storage_statistic_monthly_all.lfs_objects_size
            ) as lfs_objects_size,
            sum(
                namespace_storage_statistic_monthly_all.build_artifacts_size
            ) as build_artifacts_size,
            sum(namespace_storage_statistic_monthly_all.packages_size) as packages_size,
            sum(namespace_storage_statistic_monthly_all.wiki_size) as wiki_size,
            sum(namespace_storage_statistic_monthly_all.storage_size) as storage_size
        from namespace_lineage_monthly_all
        left join
            namespace_storage_statistic_monthly_all
            on namespace_lineage_monthly_all.namespace_id
            = namespace_storage_statistic_monthly_all.namespace_id
            and namespace_lineage_monthly_all.snapshot_month
            = namespace_storage_statistic_monthly_all.snapshot_month
        left join
            purchased_storage
            on namespace_lineage_monthly_all.namespace_id
            = purchased_storage.namespace_id
            and namespace_lineage_monthly_all.snapshot_month
            = purchased_storage.snapshot_month
        group by 1, 2

    ),
    repository_level_statistics as (

        select distinct
            namespace_lineage_monthly_all.snapshot_month,
            namespace_lineage_monthly_all.ultimate_parent_id,
            project_statistics_snapshot_monthly_all.project_id,
            coalesce(
                project_statistics_snapshot_monthly_all.project_storage_size, 0
            ) as repository_storage_size,
            iff(
                namespace_lineage_monthly_all.ultimate_parent_id = 6543, 0, 10
            ) as repository_size_limit,
            top_level_namespace_storage_summary.purchased_storage_limit,
            iff(
                repository_storage_size < repository_size_limit
                or repository_size_limit = 0,
                false,
                true
            ) as is_free_storage_used_up,
            iff(
                not is_free_storage_used_up or purchased_storage_limit = 0,
                repository_storage_size,
                repository_size_limit
            ) as free_storage_size,
            repository_storage_size - free_storage_size as purchased_storage_size,
            sum(purchased_storage_size) over (
                partition by
                    namespace_lineage_monthly_all.ultimate_parent_id,
                    namespace_lineage_monthly_all.snapshot_month
            ) as total_purchased_storage_size,
            iff(
                is_free_storage_used_up
                and (
                    purchased_storage_limit = 0
                    or total_purchased_storage_size >= purchased_storage_limit
                ),
                true,
                false
            ) as is_repository_capped
        from namespace_lineage_monthly_all
        left join
            top_level_namespace_storage_summary
            on namespace_lineage_monthly_all.ultimate_parent_id
            = top_level_namespace_storage_summary.ultimate_parent_id
            and namespace_lineage_monthly_all.snapshot_month
            = top_level_namespace_storage_summary.snapshot_month
        left join
            project_statistics_snapshot_monthly_all
            on namespace_lineage_monthly_all.namespace_id
            = project_statistics_snapshot_monthly_all.namespace_id
            and namespace_lineage_monthly_all.snapshot_month
            = project_statistics_snapshot_monthly_all.snapshot_month

    ),
    namespace_repository_storage_usage_summary as (

        select
            ultimate_parent_id,  -- Only top level namespaces
            snapshot_month,
            max(repository_storage_size) as largest_repository_size,
            sum(purchased_storage_size) as purchased_storage,
            sum(repository_size_limit) as free_limit,
            sum(free_storage_size) as free_storage,
            sum(
                iff(is_free_storage_used_up, 1, 0)
            ) as repositories_above_free_limit_count,
            sum(iff(is_repository_capped, 1, 0)) as capped_repositories_count
        from repository_level_statistics
        group by 1, 2

    ),
    joined as (

        select
            repository.snapshot_month,
            repository.ultimate_parent_id as dim_namespace_id,
            repository.ultimate_parent_id as ultimate_parent_namespace_id,
            repository.largest_repository_size as largest_repository_size_gib,
            repository.free_limit as total_free_storage_limit_gib,
            namespace.purchased_storage_limit as total_purchased_storage_limit_gib,
            iff(
                repository.repositories_above_free_limit_count = 0, false, true
            ) as has_repositories_above_free_limit,
            repository.repositories_above_free_limit_count,
            iff(
                repository.capped_repositories_count = 0, false, true
            ) as has_capped_repositories,
            repository.capped_repositories_count,
            repository.free_storage
            * {{ bytes_to_gib_conversion }} as total_free_storage_bytes,
            repository.purchased_storage
            * {{ bytes_to_gib_conversion }} as total_purchased_storage_bytes,
            namespace.billable_storage_size as billable_storage_bytes,
            namespace.repository_size as repository_bytes,
            namespace.lfs_objects_size as lfs_objects_bytes,
            namespace.build_artifacts_size as build_artifacts_bytes,
            namespace.packages_size as packages_bytes,
            namespace.wiki_size as wiki_bytes,
            namespace.storage_size as storage_bytes,
            repository.free_storage
            * {{ mib_to_gib_conversion }} as total_free_storage_mib,
            repository.purchased_storage
            * {{ mib_to_gib_conversion }} as total_purchased_storage_mib,
            namespace.billable_storage_size
            / {{ bytes_to_mib_conversion }} as billable_storage_mib,
            namespace.repository_size / {{ bytes_to_mib_conversion }} as repository_mib,
            namespace.lfs_objects_size
            / {{ bytes_to_mib_conversion }} as lfs_objects_mib,
            namespace.build_artifacts_size
            / {{ bytes_to_mib_conversion }} as build_artifacts_mib,
            namespace.packages_size / {{ bytes_to_mib_conversion }} as packages_mib,
            namespace.wiki_size / {{ bytes_to_mib_conversion }} as wiki_mib,
            namespace.storage_size / {{ bytes_to_mib_conversion }} as storage_mib,
            repository.free_storage as total_free_storage_gib,
            repository.purchased_storage as total_purchased_storage_gib,
            namespace.billable_storage_size
            / {{ bytes_to_gib_conversion }} as billable_storage_gib,
            namespace.repository_size / {{ bytes_to_gib_conversion }} as repository_gib,
            namespace.lfs_objects_size
            / {{ bytes_to_gib_conversion }} as lfs_objects_gib,
            namespace.build_artifacts_size
            / {{ bytes_to_gib_conversion }} as build_artifacts_gib,
            namespace.packages_size / {{ bytes_to_gib_conversion }} as packages_gib,
            namespace.wiki_size / {{ bytes_to_gib_conversion }} as wiki_gib,
            namespace.storage_size / {{ bytes_to_gib_conversion }} as storage_gib
        from namespace_repository_storage_usage_summary repository
        left join
            top_level_namespace_storage_summary namespace
            on repository.ultimate_parent_id = namespace.ultimate_parent_id
            and repository.snapshot_month = namespace.snapshot_month

    )

    {{
        dbt_audit(
            cte_ref="joined",
            created_by="@ischweickartDD",
            updated_by="@ischweickartDD",
            created_date="2021-01-29",
            updated_date="2021-06-17",
        )
    }}
