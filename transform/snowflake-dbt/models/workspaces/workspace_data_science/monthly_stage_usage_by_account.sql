{{ config(materialized="table", tags=["mnpi_exception"]) }}

with
    usage_ping as (select * from {{ ref("prep_usage_ping") }}),

    license_subscription_mapping as (
        select * from {{ ref("map_license_subscription_account") }}
    ),

    dates as (select * from {{ ref("dim_date") }}),

    saas_usage_ping as (select * from {{ ref("prep_saas_usage_ping_namespace") }}),

    namespace_subscription_bridge as (
        select * from {{ ref("bdg_namespace_order_subscription_monthly") }}
    ),

    usage_ping_metrics as (select * from {{ ref("dim_usage_ping_metric") }}),

    sm_last_monthly_ping_per_account as (
        select
            license_subscription_mapping.dim_crm_account_id,
            license_subscription_mapping.dim_subscription_id,
            usage_ping.dim_instance_id as uuid,
            usage_ping.host_name as hostname,
            usage_ping.raw_usage_data_payload,
            cast(usage_ping.ping_created_at_month as date) as snapshot_month
        from usage_ping
        left join
            license_subscription_mapping
            on usage_ping.license_md5
            = replace(license_subscription_mapping.license_md5, '-')
        where
            usage_ping.license_md5 is not null
            and cast(usage_ping.ping_created_at_month as date)
            < date_trunc('month', current_date)
        qualify
            row_number() over (
                partition by
                    license_subscription_mapping.dim_subscription_id,
                    usage_ping.dim_instance_id,
                    usage_ping.host_name,
                    cast(usage_ping.ping_created_at_month as date)
                order by usage_ping.ping_created_at desc
            )
            = 1
    ),

    saas_last_monthly_ping_per_account as (
        select
            namespace_subscription_bridge.dim_crm_account_id,
            namespace_subscription_bridge.dim_subscription_id,
            namespace_subscription_bridge.dim_namespace_id,
            namespace_subscription_bridge.snapshot_month,
            saas_usage_ping.ping_name as metrics_path,
            saas_usage_ping.counter_value as metrics_value
        from saas_usage_ping
        inner join dates on saas_usage_ping.ping_date = dates.date_day
        inner join
            namespace_subscription_bridge
            on saas_usage_ping.dim_namespace_id
            = namespace_subscription_bridge.dim_namespace_id
            and dates.first_day_of_month = namespace_subscription_bridge.snapshot_month
            and namespace_subscription_bridge.namespace_order_subscription_match_status
            = 'Paid All Matching'
        where
            namespace_subscription_bridge.dim_crm_account_id is not null
            and namespace_subscription_bridge.snapshot_month
            < date_trunc('month', current_date)
            and metrics_path like 'usage_activity_by_stage%'
            and metrics_value > 0  -- Filter out non-instances
        qualify
            row_number() over (
                partition by
                    namespace_subscription_bridge.dim_crm_account_id,
                    namespace_subscription_bridge.dim_namespace_id,
                    namespace_subscription_bridge.snapshot_month,
                    saas_usage_ping.ping_name
                order by saas_usage_ping.ping_date desc
            )
            = 1
    ),

    flattened_metrics as (
        select
            dim_crm_account_id,
            dim_subscription_id,
            null as dim_namespace_id,
            uuid,
            hostname,
            snapshot_month,
            "PATH" as metrics_path,
            "VALUE" as metrics_value
        from
            sm_last_monthly_ping_per_account,
            lateral flatten(input => raw_usage_data_payload, recursive => true)
        where
            metrics_path like 'usage_activity_by_stage%'
            and is_real(metrics_value) = 1
            and metrics_value > 0

        union all

        select
            dim_crm_account_id,
            dim_subscription_id,
            dim_namespace_id,
            null as uuid,
            null as hostname,
            snapshot_month,
            metrics_path,
            metrics_value
        from saas_last_monthly_ping_per_account
    )

select
    flattened_metrics.dim_crm_account_id,
    flattened_metrics.snapshot_month,

    -- NUMBER OF FEATURES USED BY PRODUCT STAGE
    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'plan'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as stage_plan_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'plan'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as stage_plan_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_stage in ('create', 'devops::create')
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as stage_create_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_stage in ('create', 'devops::create')
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as stage_create_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'verify'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as stage_verify_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'verify'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as stage_verify_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'package'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as stage_package_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'package'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as stage_package_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_stage in ('release', 'releases')
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as stage_release_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_stage in ('release', 'releases')
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as stage_release_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'configure'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as stage_configure_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'configure'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as stage_configure_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'monitor'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as stage_monitor_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'monitor'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as stage_monitor_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_stage in ('manage', 'managed')
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as stage_manage_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_stage in ('manage', 'managed')
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as stage_manage_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_stage in ('secure', 'devops::secure')
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as stage_secure_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_stage in ('secure', 'devops::secure')
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as stage_secure_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'protect'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as stage_protect_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'protect'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as stage_protect_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'ecosystem'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as stage_ecosystem_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'ecosystem'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as stage_ecosystem_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'growth'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as stage_growth_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'growth'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as stage_growth_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'enablement'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as stage_enablement_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_stage = 'enablement'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as stage_enablement_28days_features,

    -- NUMBER OF FEATURES USED BY PRODUCT STAGE
    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'dev'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as section_dev_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'dev'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as section_dev_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'enablement'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as section_enablement_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'enablement'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as section_enablement_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'fulfillment'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as section_fulfillment_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'fulfillment'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as section_fulfillment_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'growth'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as section_growth_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'growth'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as section_growth_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'ops'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as section_ops_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'ops'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as section_ops_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'sec'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as section_sec_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'sec'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as section_sec_28days_features,

    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'seg'
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as section_seg_alltime_features,
    count(
        distinct case
            when
                usage_ping_metrics.product_section = 'seg'
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as section_seg_28days_features,

    -- NUMBER OF FEATURES USED BY PRODUCT TIER
    count(
        distinct case
            when
                contains(usage_ping_metrics.tier, 'free')
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as tier_free_alltime_features,
    count(
        distinct case
            when
                contains(usage_ping_metrics.tier, 'free')
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as tier_free_28days_features,

    count(
        distinct case
            when
                contains(usage_ping_metrics.tier, 'premium')
                and not contains(usage_ping_metrics.tier, 'free')
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as tier_premium_alltime_features,
    count(
        distinct case
            when
                contains(usage_ping_metrics.tier, 'premium')
                and not contains(usage_ping_metrics.tier, 'free')
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as tier_premium_28days_features,

    count(
        distinct case
            when
                contains(usage_ping_metrics.tier, 'ultimate')
                and not contains(usage_ping_metrics.tier, 'premium')
                and usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_path
        end
    ) as tier_ultimate_alltime_features,
    count(
        distinct case
            when
                contains(usage_ping_metrics.tier, 'ultimate')
                and not contains(usage_ping_metrics.tier, 'premium')
                and usage_ping_metrics.time_frame = '28d'
            then flattened_metrics.metrics_path
        end
    ) as tier_ultimate_28days_features,

    -- NUMBER OF TIMES FEAURES ARE USED BY STAGE
    coalesce(
        sum(
            case
                when
                    usage_ping_metrics.product_stage = 'plan'
                    and usage_ping_metrics.time_frame = 'all'
                then flattened_metrics.metrics_value
            end
        ),
        0
    ) as stage_plan_alltime_feature_sum,
    coalesce(
        sum(
            case
                when
                    usage_ping_metrics.product_stage in ('create', 'devops::create')
                    and usage_ping_metrics.time_frame = 'all'
                then flattened_metrics.metrics_value
            end
        ),
        0
    ) as stage_create_alltime_feature_sum,
    coalesce(
        sum(
            case
                when
                    usage_ping_metrics.product_stage = 'verify'
                    and usage_ping_metrics.time_frame = 'all'
                then flattened_metrics.metrics_value
            end
        ),
        0
    ) as stage_verify_alltime_feature_sum,
    coalesce(
        sum(
            case
                when
                    usage_ping_metrics.product_stage = 'package'
                    and usage_ping_metrics.time_frame = 'all'
                then flattened_metrics.metrics_value
            end
        ),
        0
    ) as stage_package_alltime_feature_sum,
    coalesce(
        sum(
            case
                when
                    usage_ping_metrics.product_stage in ('release', 'releases')
                    and usage_ping_metrics.time_frame = 'all'
                then flattened_metrics.metrics_value
            end
        ),
        0
    ) as stage_release_alltime_feature_sum,
    coalesce(
        sum(
            case
                when
                    usage_ping_metrics.product_stage = 'configure'
                    and usage_ping_metrics.time_frame = 'all'
                then flattened_metrics.metrics_value
            end
        ),
        0
    ) as stage_configure_alltime_features_sum,
    coalesce(
        sum(
            case
                when
                    usage_ping_metrics.product_stage = 'monitor'
                    and usage_ping_metrics.time_frame = 'all'
                then flattened_metrics.metrics_value
            end
        ),
        0
    ) as stage_monitor_alltime_features_sum,
    coalesce(
        sum(
            case
                when
                    usage_ping_metrics.product_stage in ('manage', 'managed')
                    and usage_ping_metrics.time_frame = 'all'
                then flattened_metrics.metrics_value
            end
        ),
        0
    ) as stage_manage_alltime_feature_sum,
    coalesce(
        sum(
            case
                when
                    usage_ping_metrics.product_stage in ('secure', 'devops::secure')
                    and usage_ping_metrics.time_frame = 'all'
                then flattened_metrics.metrics_value
            end
        ),
        0
    ) as stage_secure_alltime_feature_sum,
    coalesce(
        sum(
            case
                when
                    usage_ping_metrics.product_stage = 'protect'
                    and usage_ping_metrics.time_frame = 'all'
                then flattened_metrics.metrics_value
            end
        ),
        0
    ) as stage_protect_alltime_feature_sum,
    coalesce(
        sum(
            case
                when
                    usage_ping_metrics.product_stage = 'ecosystem'
                    and usage_ping_metrics.time_frame = 'all'
                then flattened_metrics.metrics_value
            end
        ),
        0
    ) as stage_ecosystem_alltime_feature_sum,
    coalesce(
        sum(
            case
                when
                    usage_ping_metrics.product_stage = 'growth'
                    and usage_ping_metrics.time_frame = 'all'
                then flattened_metrics.metrics_value
            end
        ),
        0
    ) as stage_growth_alltime_feature_sum,
    coalesce(
        sum(
            case
                when
                    usage_ping_metrics.product_stage = 'enablement'
                    and usage_ping_metrics.time_frame = 'all'
                then flattened_metrics.metrics_value
            end
        ),
        0
    ) as stage_enablement_alltime_feature_sum,

    /* If want to calculate 28 day metrics, could use the lag function. Or
       compute by nesting this SELECT statement in a WITH and computing after
       the fact, STAGE_PLAN_ALLTIME_FEATURE_SUM -
       COALESCE(LAG(STAGE_PLAN_ALLTIME_FEATURE_SUM)
       OVER (PARTITION BY flattened_metrics.DIM_CRM_ACCOUNT_ID ORDER BY
       flattened_metrics.SNAPSHOT_MONTH), 0) as STAGE_PLAN_28DAYS_FEATURE_SUM
    */
    -- FEATURE USE SHARE BY STAGE
    sum(
        case
            when usage_ping_metrics.time_frame = 'all'
            then flattened_metrics.metrics_value
        end
    ) as all_stages_alltime_feature_sum,
    round(
        div0(stage_plan_alltime_feature_sum, all_stages_alltime_feature_sum), 4
    ) as stage_plan_alltime_share_pct,
    round(
        div0(stage_create_alltime_feature_sum, all_stages_alltime_feature_sum), 4
    ) as stage_create_alltime_share_pct,
    round(
        div0(stage_verify_alltime_feature_sum, all_stages_alltime_feature_sum), 4
    ) as stage_verify_alltime_share_pct,
    round(
        div0(stage_package_alltime_feature_sum, all_stages_alltime_feature_sum), 4
    ) as stage_package_alltime_share_pct,
    round(
        div0(stage_release_alltime_feature_sum, all_stages_alltime_feature_sum), 4
    ) as stage_release_alltime_share_pct,
    round(
        div0(stage_configure_alltime_features_sum, all_stages_alltime_feature_sum), 4
    ) as stage_configure_alltime_share_pct,
    round(
        div0(stage_monitor_alltime_features_sum, all_stages_alltime_feature_sum), 4
    ) as stage_monitor_alltime_share_pct,
    round(
        div0(stage_manage_alltime_feature_sum, all_stages_alltime_feature_sum), 4
    ) as stage_manage_alltime_share_pct,
    round(
        div0(stage_secure_alltime_feature_sum, all_stages_alltime_feature_sum), 4
    ) as stage_secure_alltime_share_pct,
    round(
        div0(stage_protect_alltime_feature_sum, all_stages_alltime_feature_sum), 4
    ) as stage_protect_alltime_share_pct,
    round(
        div0(stage_ecosystem_alltime_feature_sum, all_stages_alltime_feature_sum), 4
    ) as stage_ecosystem_alltime_share_pct,
    round(
        div0(stage_growth_alltime_feature_sum, all_stages_alltime_feature_sum), 4
    ) as stage_growth_alltime_share_pct,
    round(
        div0(stage_enablement_alltime_feature_sum, all_stages_alltime_feature_sum), 4
    ) as stage_enablement_alltime_share_pct,

    -- MOST USED STAGE ALL TIME
    case
        greatest(
            stage_plan_alltime_share_pct,
            stage_create_alltime_share_pct,
            stage_verify_alltime_share_pct,
            stage_package_alltime_share_pct,
            stage_release_alltime_share_pct,
            stage_configure_alltime_share_pct,
            stage_monitor_alltime_share_pct,
            stage_manage_alltime_share_pct,
            stage_secure_alltime_share_pct,
            stage_protect_alltime_share_pct,
            stage_ecosystem_alltime_share_pct,
            stage_growth_alltime_share_pct,
            stage_enablement_alltime_share_pct
        )
        when stage_plan_alltime_share_pct
        then 'plan'
        when stage_create_alltime_share_pct
        then 'create'
        when stage_verify_alltime_share_pct
        then 'verify'
        when stage_package_alltime_share_pct
        then 'package'
        when stage_release_alltime_share_pct
        then 'release'
        when stage_configure_alltime_share_pct
        then 'configure'
        when stage_monitor_alltime_share_pct
        then 'monitor'
        when stage_manage_alltime_share_pct
        then 'manage'
        when stage_secure_alltime_share_pct
        then 'secure'
        when stage_protect_alltime_share_pct
        then 'protect'
        when stage_ecosystem_alltime_share_pct
        then 'ecosystem'
        when stage_growth_alltime_share_pct
        then 'growth'
        when stage_enablement_alltime_share_pct
        then 'enablement'
        else 'none'
    end as stage_most_used_alltime,


    -- NUMBER OF SEAT LICENSES USING EACH STAGE
    -- Cannot get at because of the level of granuality of the usage
    -- datflattened_metrics.
    -- TOTAL MONTHS USED BY STAGES
    case
        when stage_plan_28days_features = 0
        then 0
        else
            row_number() over (
                partition by
                    flattened_metrics.dim_crm_account_id,
                    case when stage_plan_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_plan_months_used,
    case
        when stage_create_28days_features = 0
        then 0
        else
            row_number() over (
                partition by
                    flattened_metrics.dim_crm_account_id,
                    case when stage_create_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_create_months_used,
    case
        when stage_verify_28days_features = 0
        then 0
        else
            row_number() over (
                partition by
                    flattened_metrics.dim_crm_account_id,
                    case when stage_verify_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_verify_months_used,
    case
        when stage_package_28days_features = 0
        then 0
        else
            row_number() over (
                partition by
                    flattened_metrics.dim_crm_account_id,
                    case when stage_package_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_package_months_used,
    case
        when stage_release_28days_features = 0
        then 0
        else
            row_number() over (
                partition by
                    flattened_metrics.dim_crm_account_id,
                    case when stage_release_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_release_months_used,
    case
        when stage_configure_28days_features = 0
        then 0
        else
            row_number() over (
                partition by
                    flattened_metrics.dim_crm_account_id,
                    case when stage_configure_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_configure_months_used,
    case
        when stage_monitor_28days_features = 0
        then 0
        else
            row_number() over (
                partition by
                    flattened_metrics.dim_crm_account_id,
                    case when stage_monitor_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_monitor_months_used,
    case
        when stage_manage_28days_features = 0
        then 0
        else
            row_number() over (
                partition by
                    flattened_metrics.dim_crm_account_id,
                    case when stage_manage_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_manage_months_used,
    case
        when stage_secure_28days_features = 0
        then 0
        else
            row_number() over (
                partition by
                    flattened_metrics.dim_crm_account_id,
                    case when stage_secure_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_secure_months_used,
    case
        when stage_protect_28days_features = 0
        then 0
        else
            row_number() over (
                partition by
                    flattened_metrics.dim_crm_account_id,
                    case when stage_protect_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_protect_months_used,
    case
        when stage_ecosystem_28days_features = 0
        then 0
        else
            row_number() over (
                partition by
                    flattened_metrics.dim_crm_account_id,
                    case when stage_ecosystem_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_ecosystem_months_used,
    case
        when stage_growth_28days_features = 0
        then 0
        else
            row_number() over (
                partition by
                    flattened_metrics.dim_crm_account_id,
                    case when stage_growth_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_growth_months_used,
    case
        when stage_enablement_28days_features = 0
        then 0
        else
            row_number() over (
                partition by
                    flattened_metrics.dim_crm_account_id,
                    case when stage_enablement_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_enablement_months_used

from flattened_metrics
left join
    usage_ping_metrics
    on flattened_metrics.metrics_path = usage_ping_metrics.metrics_path
where
    usage_ping_metrics.metrics_status = 'active'
    and flattened_metrics.dim_crm_account_id is not null
group by flattened_metrics.dim_crm_account_id, flattened_metrics.snapshot_month
