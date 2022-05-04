{{
    simple_cte(
        [
            ("dim_crm_user_hierarchy_live", "dim_crm_user_hierarchy_live"),
            ("dim_crm_user_hierarchy_stamped", "dim_crm_user_hierarchy_stamped"),
            ("dim_sales_qualified_source", "dim_sales_qualified_source"),
            ("dim_order_type", "dim_order_type"),
            ("fct_sales_funnel_target", "fct_sales_funnel_target"),
        ]
    )
}}

,
final as (

    select
        fct_sales_funnel_target.sales_funnel_target_id,
        fct_sales_funnel_target.first_day_of_month as target_month,
        fct_sales_funnel_target.kpi_name,
        coalesce(
            dim_crm_user_hierarchy_stamped.crm_opp_owner_sales_segment_stamped,
            dim_crm_user_hierarchy_live.crm_user_sales_segment
        ) as crm_user_sales_segment,
        coalesce(
            dim_crm_user_hierarchy_stamped.crm_opp_owner_sales_segment_stamped_grouped,
            dim_crm_user_hierarchy_live.crm_user_sales_segment_grouped
        ) as crm_user_sales_segment_grouped,
        coalesce(
            dim_crm_user_hierarchy_stamped.crm_opp_owner_geo_stamped,
            dim_crm_user_hierarchy_live.crm_user_geo
        ) as crm_user_geo,
        coalesce(
            dim_crm_user_hierarchy_stamped.crm_opp_owner_region_stamped,
            dim_crm_user_hierarchy_live.crm_user_region
        ) as crm_user_region,
        coalesce(
            dim_crm_user_hierarchy_stamped.crm_opp_owner_area_stamped,
            dim_crm_user_hierarchy_live.crm_user_area
        ) as crm_user_area,
        coalesce(
            dim_crm_user_hierarchy_live.crm_user_sales_segment_region_grouped,
            dim_crm_user_hierarchy_stamped.crm_opp_owner_sales_segment_region_stamped_grouped
        ) as crm_user_sales_segment_region_grouped,
        dim_order_type.order_type_name,
        dim_order_type.order_type_grouped,
        dim_sales_qualified_source.sales_qualified_source_name,
        dim_sales_qualified_source.sales_qualified_source_grouped,
        fct_sales_funnel_target.allocated_target
    from fct_sales_funnel_target
    left join
        dim_sales_qualified_source
        on fct_sales_funnel_target.dim_sales_qualified_source_id
        = dim_sales_qualified_source.dim_sales_qualified_source_id
    left join
        dim_order_type
        on fct_sales_funnel_target.dim_order_type_id = dim_order_type.dim_order_type_id
    left join
        dim_crm_user_hierarchy_stamped
        on fct_sales_funnel_target.crm_user_sales_segment_geo_region_area
        = dim_crm_user_hierarchy_stamped.crm_opp_owner_sales_segment_geo_region_area_stamped
        and fct_sales_funnel_target.fiscal_year
        = dim_crm_user_hierarchy_stamped.fiscal_year
    left join
        dim_crm_user_hierarchy_live
        on fct_sales_funnel_target.crm_user_sales_segment_geo_region_area
        = dim_crm_user_hierarchy_live.crm_user_sales_segment_geo_region_area

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@iweeks",
        updated_by="@michellecooper",
        created_date="2021-01-08",
        updated_date="2022-03-07",
    )
}}
