{{ config(materialized="incremental", unique_key="primary_key") }}

with
    flattened_metrics as (

        select *
        from {{ ref("prep_saas_flattened_metrics") }}
        {% if is_incremental() %}
        where snapshot_month > (select max(snapshot_month) from {{ this }}) {% endif %}

    ),
    usage_ping_metrics as (select * from {{ ref("dim_usage_ping_metric") }})

select
    {{
        dbt_utils.surrogate_key(
            ["flattened_metrics.snapshot_month", "flattened_metrics.dim_namespace_id"]
        )
    }} as primary_key,
    flattened_metrics.snapshot_month,
    flattened_metrics.dim_namespace_id,

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
        div0(stage_enablement_alltime_feature_sum, all_stages_alltime_feature_sum),
        4
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
            row_number() OVER (
                partition by
                    flattened_metrics.dim_namespace_id,
                    case when stage_plan_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_plan_months_used,
    case
        when stage_create_28days_features = 0
        then 0
        else
            row_number() OVER (
                partition by
                    flattened_metrics.dim_namespace_id,
                    case when stage_create_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_create_months_used,
    case
        when stage_verify_28days_features = 0
        then 0
        else
            row_number() OVER (
                partition by
                    flattened_metrics.dim_namespace_id,
                    case when stage_verify_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_verify_months_used,
    case
        when stage_package_28days_features = 0
        then 0
        else
            row_number() OVER (
                partition by
                    flattened_metrics.dim_namespace_id,
                    case when stage_package_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_package_months_used,
    case
        when stage_release_28days_features = 0
        then 0
        else
            row_number() OVER (
                partition by
                    flattened_metrics.dim_namespace_id,
                    case when stage_release_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_release_months_used,
    case
        when stage_configure_28days_features = 0
        then 0
        else
            row_number() OVER (
                partition by
                    flattened_metrics.dim_namespace_id,
                    case when stage_configure_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_configure_months_used,
    case
        when stage_monitor_28days_features = 0
        then 0
        else
            row_number() OVER (
                partition by
                    flattened_metrics.dim_namespace_id,
                    case when stage_monitor_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_monitor_months_used,
    case
        when stage_manage_28days_features = 0
        then 0
        else
            row_number() OVER (
                partition by
                    flattened_metrics.dim_namespace_id,
                    case when stage_manage_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_manage_months_used,
    case
        when stage_secure_28days_features = 0
        then 0
        else
            row_number() OVER (
                partition by
                    flattened_metrics.dim_namespace_id,
                    case when stage_secure_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_secure_months_used,
    case
        when stage_protect_28days_features = 0
        then 0
        else
            row_number() OVER (
                partition by
                    flattened_metrics.dim_namespace_id,
                    case when stage_protect_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_protect_months_used,
    case
        when stage_ecosystem_28days_features = 0
        then 0
        else
            row_number() OVER (
                partition by
                    flattened_metrics.dim_namespace_id,
                    case when stage_ecosystem_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_ecosystem_months_used,
    case
        when stage_growth_28days_features = 0
        then 0
        else
            row_number() OVER (
                partition by
                    flattened_metrics.dim_namespace_id,
                    case when stage_growth_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_growth_months_used,
    case
        when stage_enablement_28days_features = 0
        then 0
        else
            row_number() OVER (
                partition by
                    flattened_metrics.dim_namespace_id,
                    case when stage_enablement_28days_features > 0 then 1 end
                order by flattened_metrics.snapshot_month
            )
    end as stage_enablement_months_used

from flattened_metrics
left join
    usage_ping_metrics
    on flattened_metrics.metrics_path = usage_ping_metrics.metrics_path
where usage_ping_metrics.metrics_status = 'active' {{ dbt_utils.group_by(n=3) }}
