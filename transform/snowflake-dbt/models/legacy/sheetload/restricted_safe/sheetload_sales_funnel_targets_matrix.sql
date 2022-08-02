with
    source as (select * from {{ ref("sheetload_sales_funnel_targets_matrix_source") }}),
    final as (

        select
            kpi_name,
            month,
            opportunity_source,
            order_type,
            area,
            allocated_target,
            user_segment,
            user_geo,
            user_region,
            user_area
        from source

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@iweeks",
            updated_by="@michellecooper",
            created_date="2020-11-18",
            updated_date="2022-02-10",
        )
    }}
