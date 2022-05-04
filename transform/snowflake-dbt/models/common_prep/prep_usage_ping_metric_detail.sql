{{ 
    config({
        "materialized": "table"
    })
}}

{{ simple_cte([
    ('prep_usage_ping', 'prep_usage_ping')
]) }}

, usage_ping_metrics AS (

    SELECT DISTINCT 
      TRIM(LOWER(flattened_payload.path))                                           AS metric_path,
      REPLACE(metric_path, '.','_')                                                 AS metric_path_column_name,
      'raw_usage_data_payload[''' || REPLACE(metric_path, '.', '''][''') || ''']'   AS full_metric_path,
      SPLIT_PART(metric_path, '.', 1)                                               AS main_json_name, 
      SPLIT_PART(metric_path, '.', -1)                                              AS feature_name
    FROM prep_usage_ping,
    LATERAL FLATTEN(INPUT => prep_usage_ping.raw_usage_data_payload, RECURSIVE => TRUE) flattened_payload

), final AS (

    SELECT * 
    FROM usage_ping_metrics
    WHERE feature_name != 'source_ip'

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-03-15",
    updated_date="2021-03-15"
) }}

