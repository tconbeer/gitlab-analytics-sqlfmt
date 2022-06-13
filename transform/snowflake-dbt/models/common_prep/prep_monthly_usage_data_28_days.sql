{{ config(tags=["mnpi_exception"]) }}

{{ config({"materialized": "incremental", "unique_key": "primary_key"}) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("prep_usage_ping_payload", "prep_usage_ping_payload"),
        ]
    )
}}

,
data as (

    select *
    from {{ ref("prep_usage_data_28_days_flattened") }}
    where
        typeof(metric_value) in ('INTEGER', 'DECIMAL')

        {% if is_incremental() %}

        and dim_date_id >= (select max(month_date_id) from {{ this }})

        {% endif %}

),
joined as (

    select
        ping_created_at_week,
        dim_instance_id,
        prep_usage_ping_payload.host_name as host_name,
        data.dim_date_id,
        prep_usage_ping_payload.dim_usage_ping_id,
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
        ifnull(metric_value, 0) as weekly_metrics_value,
        has_timed_out
    from data
    left join
        prep_usage_ping_payload
        on data.dim_usage_ping_id = prep_usage_ping_payload.dim_usage_ping_id

),
monthly as (

    select
        date_trunc('month', ping_created_at_week) as ping_created_month,
        dim_date.date_id as month_date_id,
        dim_instance_id,
        host_name,
        dim_usage_ping_id,
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
        weekly_metrics_value as monthly_metric_value,
        weekly_metrics_value as original_metric_value,
        has_timed_out
    from joined
    left join dim_date on date_trunc('month', ping_created_at_week) = dim_date.date_day
    qualify
        (
            row_number() over (
                partition by
                    ping_created_month, dim_instance_id, host_name, metrics_path
                order by ping_created_at_week desc, joined.dim_date_id desc
            )
        ) = 1

),
final as (

    select
        {{
            dbt_utils.surrogate_key(
                ["dim_instance_id", "host_name", "ping_created_month", "metrics_path"]
            )
        }} as primary_key,
        dim_instance_id,
        host_name,
        dim_usage_ping_id,
        ping_created_month,
        month_date_id,
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
        sum(monthly_metric_value) as monthly_metric_value,
        sum(original_metric_value) as original_metric_value,
        -- if several records and 1 has not timed out, then display FALSE
        min(has_timed_out) as has_timed_out
    from monthly {{ dbt_utils.group_by(n=16) }}

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
