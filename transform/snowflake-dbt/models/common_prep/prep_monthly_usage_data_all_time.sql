{{ config(tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("prep_usage_ping_payload", "prep_usage_ping_payload"),
        ]
    )
}},
data as (

    select *
    from {{ ref("prep_usage_data_all_time_flattened") }}
    where typeof(metric_value) in ('INTEGER', 'DECIMAL')

),
transformed as (

    select
        prep_usage_ping_payload.*,
        metrics_path,
        metric_value,
        group_name,
        stage_name,
        section_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau,
        clean_metrics_name,
        time_period,
        has_timed_out,
        date_trunc('month', ping_created_at) as ping_created_month
    from data
    left join
        prep_usage_ping_payload
        on data.dim_usage_ping_id = prep_usage_ping_payload.dim_usage_ping_id
    -- need host_name in the QUALIFY statement
    qualify
        row_number() over (
            partition by dim_instance_id, metrics_path, ping_created_month
            order by ping_created_at desc
        )
        = 1

),
monthly as (

    select
        *,
        lag(ping_created_at) over (
            partition by dim_instance_id, host_name, metrics_path
            order by ping_created_month asc
        ) as last_ping_date,
        coalesce(
            lag(metric_value) over (
                partition by dim_instance_id, host_name, metrics_path
                order by ping_created_month asc
            ),
            0
        ) as last_ping_value,
        datediff('day', last_ping_date, ping_created_at) as days_since_last_ping,
        metric_value - last_ping_value as monthly_metric_value,
        monthly_metric_value
        * 28
        / ifnull(days_since_last_ping, 1) as normalized_monthly_metric_value
    from transformed

),
final as (

    select
        {{
            dbt_utils.surrogate_key(
                ["dim_instance_id", "host_name", "ping_created_month", "metrics_path"]
            )
        }} as primary_key,
        dim_usage_ping_id,
        dim_instance_id,
        host_name,
        ping_created_month,
        dim_date.date_id as month_date_id,
        metrics_path,
        group_name,
        stage_name,
        section_name,
        is_smau,
        is_gmau,
        is_paid_gmau,
        is_umau,
        clean_metrics_name,
        time_period,
        iff(monthly_metric_value < 0, 0, monthly_metric_value) as monthly_metric_value,
        metric_value as original_metric_value,
        normalized_monthly_metric_value,
        has_timed_out
    from monthly
    left  join dim_date on monthly.ping_created_month = dim_date.date_day

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@mpeychet_",
        updated_by="@mpeychet_",
        created_date="2021-07-21",
        updated_date="2021-07-21",
    )
}}
