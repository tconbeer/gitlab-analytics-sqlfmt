{{ config({"materialized": "incremental", "unique_key": "primary_key"}) }}

with
    data as (

        select *
        from {{ ref("poc_prep_usage_data_7_days_flattened") }}
        {% if is_incremental() %}

            where created_at >= (select max(created_week) from {{ this }})

        {% endif %}

    ),
    weekly as (

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
        qualify
            (
                row_number() over (
                    partition by created_week, instance_id, host_id, metrics_path
                    order by created_at desc
                )
            )
            = 1

    ),
    transformed as (

        select
            {{
                dbt_utils.surrogate_key(
                    ["instance_id", "host_id", "created_week", "metrics_path"]
                )
            }} as primary_key,
            ping_id,
            instance_id,
            host_id,
            created_week,
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
            sum(weekly_metrics_value) as weekly_metrics_value,
            -- if several records and 1 has not timed out, then display FALSE
            min(has_timed_out) as has_timed_out
        from weekly {{ dbt_utils.group_by(n=15) }}

    )

    {{
        dbt_audit(
            cte_ref="transformed",
            created_by="@mpeychet",
            updated_by="@mpeychet",
            created_date="2021-05-04",
            updated_date="2021-05-04",
        )
    }}
