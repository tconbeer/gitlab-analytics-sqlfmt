{{ config(tags=["mnpi_exception"]) }}

with
    source_data as (

        select *
        from {{ ref("prep_sfdc_account") }}
        where dim_account_sales_segment_name_source is not null

    ),
    unioned as (

        select distinct
            {{ dbt_utils.surrogate_key(["dim_account_sales_segment_name_source"]) }}
            as dim_sales_segment_id,
            dim_account_sales_segment_name_source as sales_segment_name,
            dim_account_sales_segment_grouped_source as sales_segment_grouped
        from source_data

        union all

        select
            md5('-1') as dim_sales_segment_id,
            'Missing sales_segment_name' as sales_segment_name,
            'Missing sales_segment_grouped' as sales_segment_grouped

    )

    {{
        dbt_audit(
            cte_ref="unioned",
            created_by="@mcooperDD",
            updated_by="@jpeguero",
            created_date="2020-12-18",
            updated_date="2021-04-26",
        )
    }}
