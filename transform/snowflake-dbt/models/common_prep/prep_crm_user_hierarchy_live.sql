with
    sfdc_user as (select * from {{ ref("prep_crm_user") }} where is_active = 'TRUE'),
    final_sales_hierarchy as (

        select distinct

            {{ dbt_utils.surrogate_key(["crm_user_sales_segment_geo_region_area"]) }}
            as dim_crm_user_hierarchy_live_id,
            dim_crm_user_sales_segment_id,
            crm_user_sales_segment,
            crm_user_sales_segment_grouped,
            dim_crm_user_geo_id,
            crm_user_geo,
            dim_crm_user_region_id,
            crm_user_region,
            dim_crm_user_area_id,
            crm_user_area,
            crm_user_sales_segment_geo_region_area,
            crm_user_sales_segment_region_grouped

        from sfdc_user
        where
            crm_user_sales_segment is not null
            and crm_user_geo is not null
            and crm_user_region is not null
            and crm_user_area is not null
            and crm_user_region <> 'Sales Admin'

    )

    {{
        dbt_audit(
            cte_ref="final_sales_hierarchy",
            created_by="@mcooperDD",
            updated_by="@michellecooper",
            created_date="2020-12-18",
            updated_date="2022-02-11",
        )
    }}
