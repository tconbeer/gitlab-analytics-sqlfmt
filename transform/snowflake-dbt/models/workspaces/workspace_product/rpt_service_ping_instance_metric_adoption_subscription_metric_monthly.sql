{{ config(tags=["product", "mnpi_exception"], materialized="table") }}

{{
    simple_cte(
        [
            (
                "mart_service_ping_instance_metric_28_day",
                "mart_service_ping_instance_metric_28_day",
            ),
            (
                "potential_report_counts",
                "rpt_service_ping_instance_subcription_metric_opt_in_monthly",
            ),
            ("mart_arr", "mart_arr"),
            ("dim_service_ping_metric", "dim_service_ping_metric"),
        ]
    )
}}

-- Get value from mart_arr
,
arr_joined as (

    select mart_service_ping_instance_metric_28_day.*, mart_arr.quantity
    from mart_service_ping_instance_metric_28_day
    inner join
        mart_arr
        on mart_service_ping_instance_metric_28_day.latest_active_subscription_id
        = mart_arr.dim_subscription_id
        and mart_service_ping_instance_metric_28_day.ping_created_at_month
        = mart_arr.arr_month

-- Get actual count of subs/users for a given month/metric
),
reported_actuals as (

    select
        ping_created_at_month as arr_month,
        metrics_path as metrics_path,
        stage_name as stage_name,
        section_name as section_name,
        group_name as group_name,
        is_smau as is_smau,
        is_gmau as is_gmau,
        is_paid_gmau as is_paid_gmau,
        is_umau as is_umau,
        count(distinct latest_active_subscription_id) as subscription_count,
        sum(quantity) as seat_count
    from arr_joined
    where
        latest_active_subscription_id is not null
        and is_last_ping_of_month = true
        and service_ping_delivery_type = 'Self-Managed'
        and has_timed_out = false
        and metric_value is not null
        {{ dbt_utils.group_by(n=9) }}

-- Join actuals to number of possible subs/users
),
joined_counts as (

    select
        reported_actuals.arr_month as reporting_month,
        reported_actuals.metrics_path as metrics_path,
        reported_actuals.stage_name as stage_name,
        reported_actuals.section_name as section_name,
        reported_actuals.group_name as group_name,
        reported_actuals.is_smau as is_smau,
        reported_actuals.is_gmau as is_gmau,
        reported_actuals.is_paid_gmau as is_paid_gmau,
        reported_actuals.is_umau as is_umau,
        -- actually reported
        reported_actuals.subscription_count as reported_subscription_count,
        reported_actuals.seat_count as reported_seat_count,  -- actually reported
        -- could have reported
        potential_report_counts.total_licensed_users as total_licensed_users,
        -- could have reported
        potential_report_counts.total_subscription_count as total_subscription_count,
        total_subscription_count  -- could have reported, but didn't
        - reported_subscription_count
        as no_reporting_subscription_count,
        -- could have reported, but didn't
        total_licensed_users - reported_seat_count as no_reporting_seat_count
    from reported_actuals
    left join
        potential_report_counts
        on reported_actuals.arr_month = potential_report_counts.arr_month
        and reported_actuals.metrics_path = potential_report_counts.metrics_path

-- Split subs and seats then union
),
unioned_counts as (

    select
        reporting_month as reporting_month,
        metrics_path as metrics_path,
        stage_name as stage_name,
        section_name as section_name,
        group_name as group_name,
        is_smau as is_smau,
        is_gmau as is_gmau,
        is_paid_gmau as is_paid_gmau,
        is_umau as is_umau,
        reported_subscription_count as reporting_count,
        no_reporting_subscription_count as no_reporting_count,
        total_subscription_count as total_count,
        'metric/version check - subscription based estimation' as estimation_grain
    from joined_counts

    union all

    select
        reporting_month as reporting_month,
        metrics_path as metrics_path,
        stage_name as stage_name,
        section_name as section_name,
        group_name as group_name,
        is_smau as is_smau,
        is_gmau as is_gmau,
        is_paid_gmau as is_paid_gmau,
        is_umau as is_umau,
        reported_seat_count as reporting_count,
        no_reporting_seat_count as no_reporting_count,
        total_licensed_users as total_count,
        'metric/version check - seat based estimation' as estimation_grain
    from joined_counts

-- Create PK and use macro for percent_reporting
),
final as (

    select
        {{
            dbt_utils.surrogate_key(
                ["reporting_month", "metrics_path", "estimation_grain"]
            )
        }} as rpt_service_ping_instance_metric_adoption_subscription_metric_monthly_id,
        *,
        {{ pct_w_counters("reporting_count", "no_reporting_count") }}
        as percent_reporting
    from unioned_counts

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@icooper-acp",
        updated_by="@icooper-acp",
        created_date="2022-04-07",
        updated_date="2022-04-15",
    )
}}
