{{ config(tags=["mnpi_exception"]) }}

with
    order_type as (

        select dim_order_type_id, order_type_name, order_type_grouped
        from {{ ref("prep_order_type") }}
    )

    {{
        dbt_audit(
            cte_ref="order_type",
            created_by="@paul_armstrong",
            updated_by="@jpeguero",
            created_date="2020-11-02",
            updated_date="2021-03-23",
        )
    }}
