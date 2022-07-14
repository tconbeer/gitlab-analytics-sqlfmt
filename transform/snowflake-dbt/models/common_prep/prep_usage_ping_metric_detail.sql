{{ config({"materialized": "table"}) }}

{{ simple_cte([("prep_usage_ping", "prep_usage_ping")]) }},
usage_ping_metrics as (

    select distinct
        trim(lower(flattened_payload.path)) as metric_path,
        replace(metric_path, '.', '_') as metric_path_column_name,
        'raw_usage_data_payload['''
        || replace(metric_path, '.', '''][''')
        || ''']' as full_metric_path,
        split_part(metric_path, '.', 1) as main_json_name,
        split_part(metric_path, '.', -1) as feature_name
    from
        prep_usage_ping,
        lateral flatten(
            input => prep_usage_ping.raw_usage_data_payload,
            recursive => true
        ) flattened_payload

),
final as (select * from usage_ping_metrics where feature_name != 'source_ip')

{{
    dbt_audit(
        cte_ref="final",
        created_by="@ischweickartDD",
        updated_by="@ischweickartDD",
        created_date="2021-03-15",
        updated_date="2021-03-15",
    )
}}
