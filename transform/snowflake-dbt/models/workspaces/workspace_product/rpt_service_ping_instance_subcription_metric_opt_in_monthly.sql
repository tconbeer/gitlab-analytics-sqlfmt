{{ config(tags=["product", "mnpi_exception"], materialized="table") }}

{{
    simple_cte(
        [
            ("metric_opt_in", "rpt_service_ping_counter_statistics"),
            ("mart_arr", "mart_arr"),
            (
                "mart_service_ping_instance_metric_28_day",
                "mart_service_ping_instance_metric_28_day",
            ),
        ]
    )
}}

/*
Determine latest version for each subscription to determine if the potential metric is valid for a given month
*/
,
subscriptions_w_versions as (

    select
        ping_created_at_month as ping_created_at_month,
        dim_service_ping_instance_id as dim_service_ping_instance_id,
        latest_active_subscription_id as latest_active_subscription_id,
        ping_edition as ping_edition,
        major_minor_version as major_minor_version,
        instance_user_count as instance_user_count
    from mart_service_ping_instance_metric_28_day
    where
        is_last_ping_of_month = true
        and service_ping_delivery_type = 'Self-Managed'
        and ping_product_tier != 'Storage'
        and latest_active_subscription_id is not null
    qualify
        row_number() over (
            partition by
                ping_created_at_month,
                latest_active_subscription_id,
                dim_service_ping_instance_id
            order by major_minor_version_id desc
        ) = 1

/*
Grab just the metrics relevant to the subscription based upon version
*/
),
active_subscriptions_by_metric as (

    select
        subscriptions_w_versions.*,
        metric_opt_in.metrics_path as metrics_path,
        metric_opt_in.first_version_with_counter as first_version_with_counter,
        metric_opt_in.last_version_with_counter as last_version_with_counter
    from subscriptions_w_versions
    inner join
        metric_opt_in
        on subscriptions_w_versions.major_minor_version
        between metric_opt_in.first_version_with_counter and metric_opt_in.last_version_with_counter
        and subscriptions_w_versions.ping_edition = metric_opt_in.ping_edition

),
arr_counts_joined as (

    select active_subscriptions_by_metric.*, quantity
    from active_subscriptions_by_metric
    inner join
        mart_arr
        on active_subscriptions_by_metric.latest_active_subscription_id
        = mart_arr.dim_subscription_id
        and active_subscriptions_by_metric.ping_created_at_month = mart_arr.arr_month

/*
Aggregate for subscription and user counters
*/
),
agg_subscriptions as (

    select
        {{ dbt_utils.surrogate_key(["ping_created_at_month", "metrics_path"]) }}
        as rpt_service_ping_instance_subcription_metric_opt_in_monthly_id,
        ping_created_at_month as arr_month,
        metrics_path as metrics_path,
        count(latest_active_subscription_id) as total_subscription_count,
        sum(quantity) as total_licensed_users
    from arr_counts_joined {{ dbt_utils.group_by(n=3) }}

)

{{
    dbt_audit(
        cte_ref="agg_subscriptions",
        created_by="@icooper-acp",
        updated_by="@icooper-acp",
        created_date="2022-04-20",
        updated_date="2022-04-20",
    )
}}
