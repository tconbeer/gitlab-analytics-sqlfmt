{{ config(tags=["mnpi_exception"]) }}

with
    sales_territory as (

        select dim_sales_territory_id, sales_territory_name
        from {{ ref("prep_sales_territory") }}
    )

    {{
        dbt_audit(
            cte_ref="sales_territory",
            created_by="@paul_armstrong",
            updated_by="@mcooperDD",
            created_date="2020-10-26",
            updated_date="2020-12-18",
        )
    }}
