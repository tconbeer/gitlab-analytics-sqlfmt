{{
    simple_cte(
        [("usage_ping_metrics", "sheetload_usage_ping_metrics_sections_source")]
    )
}}

,
final as (

    select
        section_name,
        stage_name,
        group_name,
        metrics_path,
        'raw_usage_data_payload[''' || replace (
            metrics_path, '.', ''']['''
        ) || ''']' as sql_friendly_path,
        clean_metrics_name,
        periscope_metrics_name, replace (
            periscope_metrics_name, '.', '_'
        ) as sql_friendly_name,
        is_umau,
        is_smau,
        is_gmau,
        is_paid_gmau,
        time_period
    from usage_ping_metrics
    where is_smau = true or is_gmau = true or is_umau = true or is_paid_gmau = true

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@kathleentam",
        updated_by="@ischweickartDD",
        created_date="2021-03-01",
        updated_date="2021-03-15",
    )
}}
