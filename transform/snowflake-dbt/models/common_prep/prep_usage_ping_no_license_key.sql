{{ config({"materialized": "incremental", "unique_key": "dim_usage_ping_id"}) }}

with
    prep_usage_ping as (

        select * from {{ ref("prep_usage_ping") }} where license_md5 is null

    ),
    final as (

        select {{ default_usage_ping_information() }} {{ sales_wave_2_3_metrics() }}
        from prep_usage_ping

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@kathleentam",
            updated_by="@ischweickartDD",
            created_date="2021-01-11",
            updated_date="2021-04-05",
        )
    }}
