with
    project_snapshot_monthly_all as (

        -- project_snapshot_monthly 
        select
            snapshot_month,
            project_id,
            namespace_id,
            visibility_level,
            shared_runners_enabled
        from {{ ref("gitlab_dotcom_project_historical_monthly") }}
        where
            snapshot_month >= '2020-07-01' and snapshot_month < date_trunc(
                'month', current_date
            )

        UNION ALL

        -- project_current
        select
            date_trunc('month', current_date) as snapshot_month,
            project_id,
            namespace_id,
            visibility_level,
            shared_runners_enabled
        from {{ ref("gitlab_dotcom_projects_source") }}

    ),
    namespace_lineage_monthly_all as (

        -- namespace_lineage_monthly
        select
            snapshot_month,
            namespace_id,
            parent_id,
            upstream_lineage,
            ultimate_parent_id
        from {{ ref("gitlab_dotcom_namespace_lineage_historical_monthly") }}
        where
            snapshot_month >= '2020-07-01' and snapshot_month < date_trunc(
                'month', current_date
            )

        UNION ALL

        -- namespace_lineage_current
        select
            date_trunc('month', current_date) as snapshot_month,
            dim_namespace_id,
            parent_id,
            upstream_lineage,
            ultimate_parent_namespace_id
        from {{ ref("prep_namespace_lineage") }}

    ),
    namespace_snapshots_monthly_all as (

        -- namespace_snapshots_monthly
        select
            snapshot_month,
            namespace_id,
            parent_id,
            owner_id,
            namespace_type,
            visibility_level,
            shared_runners_minutes_limit,
            extra_shared_runners_minutes_limit
        from {{ ref("gitlab_dotcom_namespace_historical_monthly") }}
        where
            snapshot_month >= '2020-07-01' and snapshot_month < date_trunc(
                'month', current_date
            )

        UNION ALL

        -- namespace_current
        select
            date_trunc('month', current_date) as snapshot_month,
            namespace_id,
            parent_id,
            owner_id,
            namespace_type,
            visibility_level,
            shared_runners_minutes_limit,
            extra_shared_runners_minutes_limit
        from {{ ref("gitlab_dotcom_namespaces_source") }}

    ),
    namespace_statistics_monthly_all as (

        -- namespace_statistics_monthly
        select
            snapshot_month,
            namespace_id,
            shared_runners_seconds,
            shared_runners_seconds_last_reset
        from {{ ref("gitlab_dotcom_namespace_statistics_historical_monthly") }}
        where
            snapshot_month >= '2020-07-01' and snapshot_month < date_trunc(
                'month', current_date
            )

        UNION ALL

        -- namespace_statistics_current
        select
            date_trunc('month', current_date) as snapshot_month,
            namespace_id,
            shared_runners_seconds,
            shared_runners_seconds_last_reset
        from {{ ref("gitlab_dotcom_namespace_statistics_source") }}

    ),
    child_projects_enabled_shared_runners_any as (

        select
            project_snapshot_monthly_all.snapshot_month,
            namespace_lineage_monthly_all.ultimate_parent_id,
            max(
                project_snapshot_monthly_all.shared_runners_enabled
            ) as shared_runners_enabled
        from project_snapshot_monthly_all
        inner join
            namespace_lineage_monthly_all
            on project_snapshot_monthly_all.namespace_id
            = namespace_lineage_monthly_all.namespace_id
            and project_snapshot_monthly_all.snapshot_month
            = namespace_lineage_monthly_all.snapshot_month
        group by 1, 2

    ),
    namespace_statistics_monthly_top_level as (

        select
            namespace_snapshots_monthly_all.snapshot_month
            as namespace_snapshots_snapshot_month,
            namespace_snapshots_monthly_all.namespace_id
            as namespace_snapshots_namespace_id,
            namespace_snapshots_monthly_all.parent_id,
            namespace_snapshots_monthly_all.owner_id,
            namespace_snapshots_monthly_all.namespace_type,
            namespace_snapshots_monthly_all.visibility_level,
            namespace_snapshots_monthly_all.shared_runners_minutes_limit,
            namespace_snapshots_monthly_all.extra_shared_runners_minutes_limit,
            namespace_statistics_monthly_all.snapshot_month
            as namespace_statistics_snapshot_month,
            namespace_statistics_monthly_all.namespace_id
            as namespace_statistics_namespace_id,
            namespace_statistics_monthly_all.shared_runners_seconds,
            namespace_statistics_monthly_all.shared_runners_seconds_last_reset
        from namespace_snapshots_monthly_all
        left join
            namespace_statistics_monthly_all
            on namespace_snapshots_monthly_all.namespace_id
            = namespace_statistics_monthly_all.namespace_id
            and namespace_snapshots_monthly_all.snapshot_month
            = namespace_statistics_monthly_all.snapshot_month
            -- Only top level namespaces
            and namespace_snapshots_monthly_all.parent_id is null

    ),
    ci_minutes_logic as (

        select
            namespace_statistics_monthly_top_level.namespace_snapshots_snapshot_month
            as snapshot_month,
            namespace_statistics_monthly_top_level.namespace_snapshots_namespace_id
            as namespace_id,
            ifnull(
                child_projects_enabled_shared_runners_any.ultimate_parent_id,
                namespace_id
            ) as ultimate_parent_namespace_id,
            namespace_statistics_monthly_top_level.namespace_type,
            namespace_statistics_monthly_top_level.visibility_level,
            ifnull(
                child_projects_enabled_shared_runners_any.shared_runners_enabled, false
            ) as shared_runners_enabled,
            iff(
                snapshot_month >= '2020-10-01', 400, 2000
            ) as gitlab_current_settings_shared_runners_minutes,
            ifnull(
                namespace_statistics_monthly_top_level.shared_runners_minutes_limit,
                gitlab_current_settings_shared_runners_minutes
            ) as monthly_minutes,
            ifnull(
                namespace_statistics_monthly_top_level.extra_shared_runners_minutes_limit,
                0
            ) as purchased_minutes,
            ifnull(
                namespace_statistics_monthly_top_level.shared_runners_seconds / 60, 0
            ) as total_minutes_used,
            iff(
                purchased_minutes = 0 or total_minutes_used < monthly_minutes,
                0,
                total_minutes_used - monthly_minutes
            ) as purchased_minutes_used,
            total_minutes_used - purchased_minutes_used as monthly_minutes_used,
            iff(
                shared_runners_enabled and monthly_minutes != 0, true, false
            ) as shared_runners_minutes_limit_enabled,
            case
                when shared_runners_minutes_limit_enabled
                then monthly_minutes::varchar
                when monthly_minutes = 0
                then 'Unlimited minutes'
                else 'Not supported minutes'
            end as
        limit
            ,
            iff(monthly_minutes != 0, monthly_minutes, null) as limit_based_plan,
            case
                when not shared_runners_minutes_limit_enabled
                then 'Disabled'
                when monthly_minutes_used < monthly_minutes
                then 'Under Quota'
                else 'Over Quota'
            end as status,
            iff(
                monthly_minutes_used < monthly_minutes or monthly_minutes = 0,
                'Under Quota',
                'Over Quota'
            ) as status_based_plan,
            iff(
                purchased_minutes_used <= purchased_minutes
                or not shared_runners_minutes_limit_enabled,
                'Under Quota',
                'Over Quota'
            ) as status_purchased
        from namespace_statistics_monthly_top_level
        left join
            child_projects_enabled_shared_runners_any
            on namespace_statistics_monthly_top_level.namespace_snapshots_namespace_id
            = child_projects_enabled_shared_runners_any.ultimate_parent_id
            and namespace_statistics_monthly_top_level.namespace_snapshots_snapshot_month
            = child_projects_enabled_shared_runners_any.snapshot_month

    ),
    final as (

        select
            snapshot_month,
            namespace_id as dim_namespace_id,
            ultimate_parent_namespace_id,
            namespace_type,
            visibility_level,
        limit
            ,
            total_minutes_used as shared_runners_minutes_used_overall,
            status,
            limit_based_plan,
            monthly_minutes_used as used,
            status_based_plan,
            purchased_minutes as limit_purchased,
            purchased_minutes_used as used_purchased,
            status_purchased
        from ci_minutes_logic

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@ischweickartDD",
            updated_by="@ischweickartDD",
            created_date="2020-12-31",
            updated_date="2021-06-17",
        )
    }}
