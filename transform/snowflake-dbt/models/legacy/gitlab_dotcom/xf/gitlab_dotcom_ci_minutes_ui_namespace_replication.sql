with
    project_snapshot_monthly as (

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

    ),
    namespace_lineage_monthly as (

        select
            snapshot_month,
            namespace_id,
            parent_id,
            upstream_lineage,
            ultimate_parent_id,
            namespace_is_internal
        from {{ ref("gitlab_dotcom_namespace_lineage_historical_monthly") }}
        where
            snapshot_month >= '2020-07-01' and snapshot_month < date_trunc(
                'month', current_date
            )

    ),
    namespace_statistic_monthly as (

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

    ),
    namespace_lineage_current as (

        select
            date_trunc('month', current_date) as snapshot_month,
            namespace_id,
            parent_id,
            upstream_lineage,
            ultimate_parent_id,
            namespace_is_internal
        from {{ ref("gitlab_dotcom_namespace_lineage") }}

    ),
    namespace_snapshots_monthly as (

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

    ),
    namespace_statistic_current as (

        select
            date_trunc('month', current_date) as snapshot_month,
            namespace_id,
            shared_runners_seconds,
            shared_runners_seconds_last_reset
        from {{ ref("gitlab_dotcom_namespace_statistics") }}

    ),
    namespace_current as (

        select
            date_trunc('month', current_date) as snapshot_month,
            namespace_id,
            parent_id,
            owner_id,
            namespace_type,
            visibility_level,
            shared_runners_minutes_limit,
            extra_shared_runners_minutes_limit
        from {{ ref("gitlab_dotcom_namespaces_xf") }}

    ),
    project_current as (

        select
            date_trunc('month', current_date) as snapshot_month,
            project_id,
            namespace_id,
            visibility_level,
            shared_runners_enabled
        from {{ ref("gitlab_dotcom_projects_xf") }}

    ),
    project_snapshot_monthly_all as (

        select *
        from project_snapshot_monthly

        union

        select *
        from project_current

    ),
    namespace_lineage_monthly_all as (

        select *
        from namespace_lineage_monthly

        union

        select *
        from namespace_lineage_current

    ),
    namespace_snapshots_monthly_all as (

        select *
        from namespace_snapshots_monthly

        union

        select *
        from namespace_current

    ),
    namespace_statistic_monthly_all as (

        select *
        from namespace_statistic_monthly

        union

        select *
        from namespace_statistic_current

    ),
    child_projects_enabled_shared_runners_any as (

        select
            project_snapshot_monthly_all.snapshot_month,
            ultimate_parent_id,
            sum(iff(shared_runners_enabled, 1, 0)) as shared_runners_enabled_int,
            count(1) as project_count
        from project_snapshot_monthly_all
        inner join
            namespace_lineage_monthly_all
            on namespace_lineage_monthly_all.namespace_id
            = project_snapshot_monthly_all.namespace_id
            and namespace_lineage_monthly_all.snapshot_month
            = project_snapshot_monthly_all.snapshot_month
        group by 1, 2

    ),
    namespace_statistic_monthly_top_level as (

        select namespace_statistic_monthly_all.*
        from namespace_statistic_monthly_all
        inner join
            namespace_snapshots_monthly_all
            on namespace_statistic_monthly_all.namespace_id
            = namespace_snapshots_monthly_all.namespace_id
            and namespace_statistic_monthly_all.snapshot_month
            = namespace_snapshots_monthly_all.snapshot_month
            -- Only top level namespaces
            and namespace_snapshots_monthly_all.parent_id is null

    ),
    ci_minutes_logic as (

        select
            namespace_statistic_monthly_top_level.snapshot_month,
            namespace_statistic_monthly_top_level.namespace_id,
            shared_runners_minutes_limit,
            extra_shared_runners_minutes_limit,
            2000 as gitlab_current_settings_shared_runners_minutes,
            child_projects_enabled_shared_runners_any.shared_runners_enabled_int,
            project_count,
            namespace_statistic_monthly_top_level.shared_runners_seconds
            as shared_runners_seconds_used,
            shared_runners_seconds_used / 60 as shared_runners_minutes_used,
            iff(
                namespace_snapshots_monthly_all.parent_id is null, true, false
            ) as has_parent_not,
            iff(has_parent_not, true, false) as shared_runners_minutes_supported,
            iff(
                child_projects_enabled_shared_runners_any.shared_runners_enabled_int
                > 0,
                true,
                false
            ) as shared_runners_enabled,
            iff(
                iff(
                    shared_runners_minutes_limit is not null,
                    shared_runners_minutes_limit + ifnull(
                        extra_shared_runners_minutes_limit, 0
                    ),
                    gitlab_current_settings_shared_runners_minutes + ifnull(
                        extra_shared_runners_minutes_limit, 0
                    )
                ) > 0,
                true,
                false
            ) as actual_shared_runners_minutes_limit_non_zero,
            iff(
                shared_runners_minutes_supported
                and shared_runners_enabled
                and actual_shared_runners_minutes_limit_non_zero,
                true,
                false
            ) as shared_runners_minutes_limit_enabled,
            shared_runners_minutes_used as minutes_used,
            ifnull(
                shared_runners_minutes_limit,
                gitlab_current_settings_shared_runners_minutes
            ) as monthly_minutes,
            ifnull(extra_shared_runners_minutes_limit, 0) as purchased_minutes,
            iff(purchased_minutes = 0, true, false) as no_minutes_purchased,
            iff(
                minutes_used <= monthly_minutes, true, false
            ) as monthly_minutes_available,
            iff(
                no_minutes_purchased or monthly_minutes_available,
                0,
                minutes_used - monthly_minutes
            ) as purchased_minutes_used,
            minutes_used - purchased_minutes_used as monthly_minutes_used,
            iff(
                shared_runners_minutes_limit_enabled
                and monthly_minutes_used >= monthly_minutes,
                true,
                false
            ) as monthly_minutes_used_up,
            iff(purchased_minutes > 0, true, false) as any_minutes_purchased,
            iff(
                shared_runners_minutes_limit_enabled
                and any_minutes_purchased
                and purchased_minutes_used >= purchased_minutes,
                true,
                false
            ) as purchased_minutes_used_up,
            monthly_minutes_used as used,
            iff(
                shared_runners_minutes_limit_enabled,
                monthly_minutes::varchar,
                'Unlimited'
            ) as
        limit
            ,
            monthly_minutes::varchar as limit_based_plan,
            case
                when shared_runners_minutes_limit_enabled
                then iff(monthly_minutes_used_up, 'Over Quota', 'Under Quota')
                else 'Disabled'
            end as status,
            iff(
                monthly_minutes_used >= monthly_minutes, 'Over Quota', 'Under Quota'
            ) as status_based_plan,
            purchased_minutes_used as used_purchased,
            purchased_minutes as limit_purchased,
            iff(
                purchased_minutes_used_up, 'Over Quota', 'Under Quota'
            ) as status_purchased

        from namespace_statistic_monthly_top_level
        inner join
            namespace_snapshots_monthly_all
            on namespace_snapshots_monthly_all.snapshot_month
            = namespace_statistic_monthly_top_level.snapshot_month
            and namespace_snapshots_monthly_all.namespace_id
            = namespace_statistic_monthly_top_level.namespace_id
        left join
            child_projects_enabled_shared_runners_any
            on child_projects_enabled_shared_runners_any.snapshot_month
            = namespace_statistic_monthly_top_level.snapshot_month
            and child_projects_enabled_shared_runners_any.ultimate_parent_id
            = namespace_statistic_monthly_top_level.namespace_id

    )

select
    snapshot_month,
    namespace_id,
    shared_runners_minutes_used as shared_runners_minutes_used_overall,
    used,
limit
    ,
    limit_based_plan,
    status,
    status_based_plan,
    used_purchased,
    limit_purchased,
    status_purchased
from ci_minutes_logic
