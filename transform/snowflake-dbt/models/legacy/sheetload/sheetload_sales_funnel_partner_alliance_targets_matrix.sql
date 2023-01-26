with
    source as (

        select *
        from {{ ref("sheetload_sales_funnel_partner_alliance_targets_matrix_source") }}

    ),
    final as (

        select
            kpi_name,
            month,
            sales_qualified_source,
            alliance_partner,
            order_type,
            area,
            allocated_target
        from source

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@jpeguero",
            updated_by="@jpeguero",
            created_date="2021-04-05",
            updated_date="2021-09-10",
        )
    }}
