{{ config(tags=["product", "mnpi_exception"]) }}

{{ config({"materialized": "incremental", "unique_key": "primary_key"}) }}

with
    flattened_data as (select * from {{ ref("prep_usage_data_7_days_flattened") }}),
    prep_usage_ping_payload as (select * from {{ ref("prep_usage_ping_payload") }}),
    weekly as (

        select
            date_trunc('week', ping_created_at) as created_week,
            prep_usage_ping_payload.dim_usage_ping_id,
            ping_created_at,
            prep_usage_ping_payload.dim_instance_id,
            host_name,
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
        from flattened_data
        left join
            prep_usage_ping_payload
            on flattened_data.dim_usage_ping_id
            = prep_usage_ping_payload.dim_usage_ping_id
        qualify
            (
                row_number() over (
                    partition by created_week, dim_instance_id, host_name, metrics_path
                    order by ping_created_at desc
                )
            )
            = 1

    ),
    transformed as (

        select
            {{
                dbt_utils.surrogate_key(
                    ["dim_instance_id", "host_name", "created_week", "metrics_path"]
                )
            }} as primary_key,
            dim_usage_ping_id,
            dim_instance_id,
            host_name,
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
