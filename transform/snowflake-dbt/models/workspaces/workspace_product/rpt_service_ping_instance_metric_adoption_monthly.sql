{{ config(tags=["product", "mnpi_exception"], materialized="table") }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("mart_arr", "mart_arr"),
            (
                "mart_service_ping_instance_metric_28_day",
                "mart_service_ping_instance_metric_28_day",
            ),
            ("dim_service_ping_metric", "dim_service_ping_metric"),
        ]
    )
}}

,
subscription_info as (

    select
        arr_month,
        sum(arr) as arr,
        sum(quantity) as total_licensed_users,
        count(distinct dim_subscription_id) as total_subscriptions_count,
        1 as key
    from mart_arr
    where
        arr_month = '2022-01-01'
        and product_tier_name != 'Storage'
        and product_delivery_type = 'Self-Managed'
    group by 1
    order by 1 desc

),
metrics as (select *, 1 as key from dim_service_ping_metric),
sub_combo as (

    select subscription_info.*, metrics_path
    from subscription_info
    inner join metrics on subscription_info.key = metrics.key

),
count_tbl as (

    select
        dim_date.first_day_of_month as arr_month,
        metrics_path as metrics_path,
        stage_name as stage_name,
        section_name as section_name,
        group_name as group_name,
        is_smau as is_smau,
        is_gmau as is_gmau,
        is_paid_gmau as is_paid_gmau,
        is_umau as is_umau,
        count(latest_active_subscription_id) as subscription_count,
        sum(instance_user_count) as seat_count
    from mart_service_ping_instance_metric_28_day
    inner join
        dim_date
        on mart_service_ping_instance_metric_28_day.dim_service_ping_date_id
        = dim_date.date_id
    where
        latest_active_subscription_id is not null
        and is_last_ping_of_month = true
        and service_ping_delivery_type = 'Self-Managed'
        and has_timed_out = false
        and metric_value is not null
        {{ dbt_utils.group_by(n=9) }}


),
joined_counts as (

    select
        count_tbl.arr_month as reporting_month,
        count_tbl.metrics_path as metrics_path,
        count_tbl.stage_name as stage_name,
        count_tbl.section_name as section_name,
        count_tbl.group_name as group_name,
        count_tbl.is_smau as is_smau,
        count_tbl.is_gmau as is_gmau,
        count_tbl.is_paid_gmau as is_paid_gmau,
        count_tbl.is_umau as is_umau,
        count_tbl.subscription_count as reported_subscription_count,
        count_tbl.seat_count as reported_seat_count,
        sub_combo.total_licensed_users as total_licensed_users,
        sub_combo.total_subscriptions_count as total_subscriptions_count,
        total_subscriptions_count
        - reported_subscription_count
        as no_reporting_subscription_count,
        total_licensed_users - reported_seat_count as no_reporting_seat_count
    from count_tbl
    left join
        sub_combo
        on count_tbl.arr_month = sub_combo.arr_month
        and count_tbl.metrics_path = sub_combo.metrics_path

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
        total_subscriptions_count as total_count,
        'subscription based estimation' as estimation_grain
    from joined_counts

    UNION ALL

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
        'seat based estimation' as estimation_grain
    from joined_counts

),
final as (

    select
        {{
            dbt_utils.surrogate_key(
                ["reporting_month", "metrics_path", "estimation_grain"]
            )
        }} as rpt_service_ping_instance_metric_adoption_monthly_id,
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
