{{
    simple_cte(
        [
            ("dim_crm_user_hierarchy_live", "dim_crm_user_hierarchy_live"),
            ("dim_sales_qualified_source", "dim_sales_qualified_source"),
            ("dim_order_type", "dim_order_type"),
            ("fct_sales_funnel_target", "fct_sales_funnel_target"),
            ("dim_date", "dim_date"),
            ("dim_crm_user_hierarchy_stamped", "dim_crm_user_hierarchy_stamped"),
        ]
    )
}},
monthly_targets as (

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
            dim_crm_user_hierarchy_stamped.crm_opp_owner_sales_segment_region_stamped_grouped,
            dim_crm_user_hierarchy_live.crm_user_sales_segment_region_grouped
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

),
monthly_targets_daily as (

    select
        date_day,
        monthly_targets.*,
        datediff('day', first_day_of_month, last_day_of_month) + 1 as days_of_month,
        first_day_of_week,
        fiscal_quarter_name,
        fiscal_year,
        allocated_target / days_of_month as daily_allocated_target
    from monthly_targets
    inner join dim_date on monthly_targets.target_month = dim_date.first_day_of_month

),
qtd_mtd_target as (

    select
        {{
            dbt_utils.surrogate_key(
                [
                    "date_day",
                    "kpi_name",
                    "crm_user_sales_segment",
                    "crm_user_geo",
                    "crm_user_region",
                    "crm_user_area",
                    "order_type_name",
                    "sales_qualified_source_name",
                ]
            )
        }} as primary_key,
        date_day as target_date,
        dateadd('day', 1, target_date) as report_target_date,
        first_day_of_week,
        target_month,
        fiscal_quarter_name,
        fiscal_year,
        kpi_name,
        crm_user_sales_segment,
        crm_user_sales_segment_grouped,
        crm_user_geo,
        crm_user_region,
        crm_user_area,
        crm_user_sales_segment_region_grouped,
        order_type_name,
        order_type_grouped,
        sales_qualified_source_name,
        sales_qualified_source_grouped,
        allocated_target as monthly_allocated_target,
        daily_allocated_target,
        sum(daily_allocated_target) OVER (
            partition by
                kpi_name,
                crm_user_sales_segment,
                crm_user_geo,
                crm_user_region,
                crm_user_area,
                order_type_name,
                sales_qualified_source_name,
                first_day_of_week
            order by date_day
        ) as wtd_allocated_target,
        sum(daily_allocated_target) OVER (
            partition by
                kpi_name,
                crm_user_sales_segment,
                crm_user_geo,
                crm_user_region,
                crm_user_area,
                order_type_name,
                sales_qualified_source_name,
                target_month
            order by date_day
        ) as mtd_allocated_target,
        sum(daily_allocated_target) OVER (
            partition by
                kpi_name,
                crm_user_sales_segment,
                crm_user_geo,
                crm_user_region,
                crm_user_area,
                order_type_name,
                sales_qualified_source_name,
                fiscal_quarter_name
            order by date_day
        ) as qtd_allocated_target,
        sum(daily_allocated_target) OVER (
            partition by
                kpi_name,
                crm_user_sales_segment,
                crm_user_geo,
                crm_user_region,
                crm_user_area,
                order_type_name,
                sales_qualified_source_name,
                fiscal_year
            order by date_day
        ) as ytd_allocated_target

    from monthly_targets_daily

)

{{
    dbt_audit(
        cte_ref="qtd_mtd_target",
        created_by="@jpeguero",
        updated_by="@michellecooper",
        created_date="2021-02-18",
        updated_date="2022-03-07",
    )
}}
