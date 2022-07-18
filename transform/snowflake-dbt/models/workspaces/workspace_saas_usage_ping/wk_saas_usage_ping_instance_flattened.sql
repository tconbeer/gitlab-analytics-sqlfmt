{{ config({"materialized": "table"}) }}

{{
    simple_cte(
        [
            ("saas_usage_ping_instance", "saas_usage_ping_instance"),
            ("dim_usage_ping_metric", "dim_usage_ping_metric"),
        ]
    )
}},
flattened as (

    select
        saas_usage_ping_gitlab_dotcom_id as saas_usage_ping_gitlab_dotcom_id,
        ping_date as ping_date,
        coalesce(try_parse_json(path)[0]::text, path::text) as metric_path,
        value::text as metric_value,
        recorded_at as recorded_at,
        version as version,
        edition as edition,
        recording_ce_finished_at as recording_ce_finished_at,
        recording_ee_finished_at as recording_ee_finished_at,
        uuid as uuid,
        _uploaded_at as _uploaded_at
    from
        saas_usage_ping_instance,
        lateral flatten(input => run_results, recursive => true)

),
joined as (

    select
        flattened.saas_usage_ping_gitlab_dotcom_id as saas_usage_ping_gitlab_dotcom_id,
        flattened.ping_date as ping_date,
        flattened.metric_path as metric_path,
        flattened.metric_value as metric_value,
        dim_usage_ping_metric.metrics_status as metric_status,
        flattened.recorded_at as recorded_at,
        flattened.version as version,
        flattened.edition as edition,
        flattened.recording_ce_finished_at as recording_ce_finished_at,
        flattened.recording_ee_finished_at as recording_ee_finished_at,
        flattened.uuid as uuid,
        flattened._uploaded_at as _uploaded_at
    from flattened
    left join
        dim_usage_ping_metric
        on flattened.metric_path = dim_usage_ping_metric.metrics_path

)
select *
from joined
