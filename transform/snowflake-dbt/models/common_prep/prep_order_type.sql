{{ config(tags=["mnpi_exception"]) }}

with
    source_data as (

        select *
        from {{ ref("sfdc_opportunity_source") }}
        where order_type_stamped is not null and not is_deleted

    ),
    unioned as (

        select distinct
            {{ dbt_utils.surrogate_key(["order_type_stamped"]) }} as dim_order_type_id,
            order_type_stamped as order_type_name,
            order_type_grouped
        from source_data

        union all

        select
            md5('-1') as dim_order_type_id,
            'Missing order_type_name' as order_type_name,
            'Missing order_type_grouped' as order_type_grouped

    )

    {{
        dbt_audit(
            cte_ref="unioned",
            created_by="@mcooperDD",
            updated_by="@jpeguero",
            created_date="2020-12-18",
            updated_date="2021-03-23",
        )
    }}
