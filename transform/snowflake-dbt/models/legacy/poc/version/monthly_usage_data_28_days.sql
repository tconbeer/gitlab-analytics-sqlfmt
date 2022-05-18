{{ config({"materialized": "incremental", "unique_key": "primary_key"}) }}

with
    data as (

        select *
        from {{ ref("poc_prep_usage_data_28_days_flattened") }}
        where
            typeof(metric_value) in ('INTEGER', 'DECIMAL')

            {% if is_incremental() %}

            and created_at >= (select max(created_month) from {{ this }})

            {% endif %}

    ),
    transformed as (

        select
            date_trunc('week', created_at) as created_week,
            ping_id,
            created_at,
            instance_id,
            host_id,
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

    ),
    monthly as (

        select
            date_trunc('month', created_week) as created_month,
            instance_id,
            host_id,
            ping_id,
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
        from transformed
        qualify
            (
                row_number() over (
                    partition by created_month, instance_id, host_id, metrics_path
                    order by created_week desc, created_at desc
                )
            ) = 1

    )

select
    {{
        dbt_utils.surrogate_key(
            ["instance_id", "host_id", "created_month", "metrics_path"]
        )
    }} as primary_key,
    ping_id,
    instance_id,
    host_id,
    created_month,
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
from monthly {{ dbt_utils.group_by(n=15) }}
