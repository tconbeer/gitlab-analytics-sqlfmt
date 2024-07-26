{{ config(tags=["product", "mnpi_exception"]) }}

{{ config({"materialized": "incremental", "unique_key": "instance_path_id"}) }}

with
    flattened as (select * from {{ ref("prep_usage_data_flattened") }}),
    usage_ping_metrics as (select * from {{ ref("dim_usage_ping_metric") }}),
    joined as (

        select
            flattened.instance_path_id,
            flattened.dim_usage_ping_id,
            flattened.metrics_path,
            metrics.section_name,
            metrics.stage_name,
            metrics.group_name,
            coalesce(metrics.is_smau, false) as is_smau,
            coalesce(metrics.is_gmau, false) as is_gmau,
            metrics.clean_metrics_name,
            metrics.periscope_metrics_name,
            metrics.time_period,
            coalesce(metrics.is_umau, false) as is_umau,
            coalesce(metrics.is_paid_gmau, false) as is_paid_gmau,
            iff(flattened.metric_value = -1, 0, flattened.metric_value) as metric_value,
            iff(flattened.metric_value = -1, true, false) as has_timed_out,
            time_frame
        from flattened
        inner join
            usage_ping_metrics
            on flattened.metrics_path = usage_ping_metrics.metrics_path
            and time_frame = '7d'
        left join
            {{ ref("sheetload_usage_ping_metrics_sections") }} as metrics
            on flattened.metrics_path = metrics.metrics_path

    )

    {{
        dbt_audit(
            cte_ref="joined",
            created_by="@mpeychet",
            updated_by="@mpeychet",
            created_date="2021-05-04",
            updated_date="2021-05-04",
        )
    }}
