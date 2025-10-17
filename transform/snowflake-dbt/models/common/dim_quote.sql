{{ config(tags=["mnpi_exception"]) }}

with
    quote as (

        select
            dim_quote_id,
            quote_number,
            quote_name,
            quote_status,
            is_primary_quote,
            quote_start_date
        from {{ ref("prep_quote") }}

    )

    {{
        dbt_audit(
            cte_ref="quote",
            created_by="@snalamaru",
            updated_by="@snalamaru",
            created_date="2021-01-07",
            updated_date="2021-01-07",
        )
    }}
