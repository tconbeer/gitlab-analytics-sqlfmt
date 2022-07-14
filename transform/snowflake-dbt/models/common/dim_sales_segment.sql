{{ config(tags=["mnpi_exception"]) }}

with
    sales_segment as (

        select dim_sales_segment_id, sales_segment_name, sales_segment_grouped
        from {{ ref("prep_sales_segment") }}
    )

    {{
        dbt_audit(
            cte_ref="sales_segment",
            created_by="@msendal",
            updated_by="@jpeguero",
            created_date="2020-11-05",
            updated_date="2020-04-26",
        )
    }}
