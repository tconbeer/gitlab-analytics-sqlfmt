with
    crm_account_dimensions as (select * from {{ ref("map_crm_account") }}),
    order_type as (select * from {{ ref("dim_order_type") }}),
    sales_qualified_source as (select * from {{ ref("dim_sales_qualified_source") }}),
    deal_path as (select * from {{ ref("dim_deal_path") }}),
    sales_segment as (select * from {{ ref("dim_sales_segment") }}),
    sfdc_opportunity as (select * from {{ ref("sfdc_opportunity") }}),
    opportunity_fields as (

        select

            opportunity_id as dim_crm_opportunity_id,
            account_id as dim_crm_account_id,
            owner_id as dim_crm_user_id,
            deal_path,
            order_type_stamped as order_type,
            sales_segment,
            sales_qualified_source

        from sfdc_opportunity

    ),
    opportunities_with_keys as (

        select
            opportunity_fields.dim_crm_opportunity_id,
            {{ get_keyed_nulls("opportunity_fields.dim_crm_user_id") }}
            as dim_crm_user_id,
            {{ get_keyed_nulls("order_type.dim_order_type_id") }} as dim_order_type_id,
            {{ get_keyed_nulls("sales_qualified_source.dim_sales_qualified_source_id") }}
            as dim_sales_qualified_source_id,
            {{ get_keyed_nulls("deal_path.dim_deal_path_id") }} as dim_deal_path_id,
            crm_account_dimensions.dim_parent_crm_account_id,
            crm_account_dimensions.dim_crm_account_id,
            crm_account_dimensions.dim_parent_sales_segment_id,
            crm_account_dimensions.dim_parent_sales_territory_id,
            crm_account_dimensions.dim_parent_industry_id,
            crm_account_dimensions.dim_parent_location_country_id,
            crm_account_dimensions.dim_parent_location_region_id,
            {{
                get_keyed_nulls(
                    "crm_account_dimensions.dim_account_sales_segment_id,sales_segment.dim_sales_segment_id"
                )
            }}
            as dim_account_sales_segment_id,
            crm_account_dimensions.dim_account_sales_territory_id,
            crm_account_dimensions.dim_account_industry_id,
            crm_account_dimensions.dim_account_location_country_id,
            crm_account_dimensions.dim_account_location_region_id

        from opportunity_fields
        left join
            crm_account_dimensions
            on opportunity_fields.dim_crm_account_id
            = crm_account_dimensions.dim_crm_account_id
        left join
            sales_qualified_source
            on opportunity_fields.sales_qualified_source
            = sales_qualified_source.sales_qualified_source_name
        left join
            order_type on opportunity_fields.order_type = order_type.order_type_name
        left join deal_path on opportunity_fields.deal_path = deal_path.deal_path_name
        left join
            sales_segment
            on opportunity_fields.sales_segment = sales_segment.sales_segment_name

    )

    {{
        dbt_audit(
            cte_ref="opportunities_with_keys",
            created_by="@snalamaru",
            updated_by="@iweeks",
            created_date="2020-12-17",
            updated_date="2021-04-22",
        )
    }}
