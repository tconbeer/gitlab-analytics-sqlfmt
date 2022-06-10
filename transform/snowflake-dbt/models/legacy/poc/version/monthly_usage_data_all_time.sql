{{ config({"materialized": "incremental", "unique_key": "primary_key"}) }}

with
    data as (

        select *
        from {{ ref("poc_prep_usage_data_all_time_flattened") }}
        where
            typeof(metric_value) in ('INTEGER', 'DECIMAL')

            {% if is_incremental() %}

            and created_at >= (
                select dateadd('month', -1, max(created_month)) from {{ this }}
            )

            {% endif %}

    )

    ,
    transformed as (

        select *, date_trunc('month', created_at) as created_month
        from data
        qualify
            row_number() over (
                partition by instance_id, metrics_path, host_id, created_month
                order by created_at desc
            ) = 1

    )

    ,
    monthly as (

        select
            *,
            metric_value - coalesce(
                lag(metric_value) over (
                    partition by instance_id, host_id, metrics_path
                    order by created_month
                ),
                0
            ) as monthly_metric_value
        from transformed

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
    iff(monthly_metric_value < 0, 0, monthly_metric_value) as monthly_metric_value,
    metric_value as original_metric_value,
    has_timed_out
from monthly
{% if is_incremental() %}

where created_month >= (select max(created_month) from {{ this }})

{% endif %}
