{{ config(tags=["mnpi_exception"]) }}

with
    source_data as (

        select *
        from {{ ref("sfdc_opportunity_source") }}
        where sales_qualified_source is not null and not is_deleted

    ),
    unioned as (

        select distinct
            md5(
                cast(coalesce(cast(sales_qualified_source as varchar), '') as varchar)
            ) as dim_sales_qualified_source_id,
            sales_qualified_source as sales_qualified_source_name,
            sales_qualified_source_grouped as sales_qualified_source_grouped,
            sqs_bucket_engagement
        from source_data

        UNION ALL

        select
            md5('-1') as dim_sales_qualified_source_id,
            'Missing sales_qualified_source_name' as sales_qualified_source_name,
            'Web Direct Generated' as sales_qualified_source_grouped,
            'Co-sell' as sqs_bucket_engagement

    )

    {{
        dbt_audit(
            cte_ref="unioned",
            created_by="@mcooperDD",
            updated_by="@jpeguero",
            created_date="2020-10-26",
            updated_date="2021-09-09",
        )
    }}
