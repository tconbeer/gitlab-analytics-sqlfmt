{{
    simple_cte(
        [
            ("sfdc_user_hierarchy_live", "prep_crm_user_hierarchy_live"),
            ("sfdc_user_hierarchy_stamped", "prep_crm_user_hierarchy_stamped"),
            ("sales_qualified_source", "prep_sales_qualified_source"),
            ("order_type", "prep_order_type"),
            ("alliance_type", "prep_alliance_type"),
            ("channel_type", "prep_channel_type"),
            ("date_details_source", "date_details_source"),
        ]
    )
}},
date as (

    select distinct fiscal_month_name_fy, fiscal_year, first_day_of_month
    from date_details_source

),
sheetload_sales_funnel_partner_alliance_targets_matrix_source as (

    select
        sheetload_sales_funnel_partner_alliance_targets_matrix_source.*,
        {{
            channel_type(
                "sheetload_sales_funnel_partner_alliance_targets_matrix_source.sqs_bucket_engagement",
                "sheetload_sales_funnel_partner_alliance_targets_matrix_source.order_type",
            )
        }}
        as channel_type
    from {{ ref("sheetload_sales_funnel_partner_alliance_targets_matrix_source") }}

),
target_matrix as (

    select
        sheetload_sales_funnel_partner_alliance_targets_matrix_source.*,
        date.first_day_of_month,
        date.fiscal_year,
        {{ get_keyed_nulls("sales_qualified_source.dim_sales_qualified_source_id") }}
        as dim_sales_qualified_source_id,
        {{ get_keyed_nulls("order_type.dim_order_type_id") }} as dim_order_type_id,
        {{ get_keyed_nulls("alliance_type.dim_alliance_type_id") }}
        as dim_alliance_type_id,
        {{ get_keyed_nulls("channel_type.dim_channel_type_id") }} as dim_channel_type_id
    from sheetload_sales_funnel_partner_alliance_targets_matrix_source
    left join
        date
        on {{
            sales_funnel_text_slugify(
                "sheetload_sales_funnel_partner_alliance_targets_matrix_source.month"
            )
        }} = {{ sales_funnel_text_slugify("date.fiscal_month_name_fy") }}
    left join
        sales_qualified_source
        on
        {{
            sales_funnel_text_slugify(
                "sheetload_sales_funnel_partner_alliance_targets_matrix_source.sales_qualified_source"
            )
        }}
        = {{
            sales_funnel_text_slugify(
                "sales_qualified_source.sales_qualified_source_name"
            )
        }}
    left join
        order_type
        on
        {{
            sales_funnel_text_slugify(
                "sheetload_sales_funnel_partner_alliance_targets_matrix_source.order_type"
            )
        }}
        = {{ sales_funnel_text_slugify("order_type.order_type_name") }}
    left join
        alliance_type
        on
        {{
            sales_funnel_text_slugify(
                "sheetload_sales_funnel_partner_alliance_targets_matrix_source.alliance_partner"
            )
        }}
        = {{ sales_funnel_text_slugify("alliance_type.alliance_type_name") }}
    left join
        channel_type
        on
        {{
            sales_funnel_text_slugify(
                "sheetload_sales_funnel_partner_alliance_targets_matrix_source.channel_type"
            )
        }}
        = {{ sales_funnel_text_slugify("channel_type.channel_type_name") }}

),
fy22_user_hierarchy as (
    /* 
For FY22, targets in the sheetload file were set at the user_area grain, so we join to the stamped hierarchy on the user_area. We also want to find the last user_area in the fiscal year
because if there were multiple hierarchies for this user_area, the last one created is assumed to be the correct version. It is necessary to have a 1:1 relationship between area in the target
sheetload and user_area in the hierarchy so the targets do not fan out.
*/
    select *
    from sfdc_user_hierarchy_stamped
    where fiscal_year = 2022 and is_last_user_area_in_fiscal_year = 1

),
fy23_and_beyond_user_hierarchy as (
    /* 
For FY23 and beyond, targets in the sheetload file were set at the user_segment_geo_region_area grain, so we join to the stamped hierarchy on the user_segment_geo_region_area.
*/
    select *
    from sfdc_user_hierarchy_stamped
    where fiscal_year > 2022 and is_last_user_hierarchy_in_fiscal_year = 1

),
unioned_targets as (

    select
        target_matrix.kpi_name,
        target_matrix.first_day_of_month,
        target_matrix.dim_sales_qualified_source_id,
        target_matrix.sales_qualified_source,
        target_matrix.dim_order_type_id,
        target_matrix.order_type,
        target_matrix.fiscal_year,
        target_matrix.allocated_target,
        target_matrix.channel_type,
        target_matrix.dim_channel_type_id,
        target_matrix.alliance_partner,
        target_matrix.dim_alliance_type_id,
        fy22_user_hierarchy.crm_opp_owner_sales_segment_geo_region_area_stamped,
        fy22_user_hierarchy.dim_crm_user_hierarchy_stamped_id,
        fy22_user_hierarchy.dim_crm_opp_owner_sales_segment_stamped_id,
        fy22_user_hierarchy.crm_opp_owner_sales_segment_stamped,
        fy22_user_hierarchy.dim_crm_opp_owner_geo_stamped_id,
        fy22_user_hierarchy.crm_opp_owner_geo_stamped,
        fy22_user_hierarchy.dim_crm_opp_owner_region_stamped_id,
        fy22_user_hierarchy.crm_opp_owner_region_stamped,
        fy22_user_hierarchy.dim_crm_opp_owner_area_stamped_id,
        fy22_user_hierarchy.crm_opp_owner_area_stamped
    from target_matrix
    left join
        fy22_user_hierarchy
        on {{ sales_funnel_text_slugify("target_matrix.area") }}
        =
        {{ sales_funnel_text_slugify("fy22_user_hierarchy.crm_opp_owner_area_stamped") }}
    where target_matrix.fiscal_year = 2022

    union all

    select
        target_matrix.kpi_name,
        target_matrix.first_day_of_month,
        target_matrix.dim_sales_qualified_source_id,
        target_matrix.sales_qualified_source,
        target_matrix.dim_order_type_id,
        target_matrix.order_type,
        target_matrix.fiscal_year,
        target_matrix.allocated_target,
        target_matrix.channel_type,
        target_matrix.dim_channel_type_id,
        target_matrix.alliance_partner,
        target_matrix.dim_alliance_type_id,
        fy23_and_beyond_user_hierarchy.crm_opp_owner_sales_segment_geo_region_area_stamped,
        fy23_and_beyond_user_hierarchy.dim_crm_user_hierarchy_stamped_id,
        fy23_and_beyond_user_hierarchy.dim_crm_opp_owner_sales_segment_stamped_id,
        fy23_and_beyond_user_hierarchy.crm_opp_owner_sales_segment_stamped,
        fy23_and_beyond_user_hierarchy.dim_crm_opp_owner_geo_stamped_id,
        fy23_and_beyond_user_hierarchy.crm_opp_owner_geo_stamped,
        fy23_and_beyond_user_hierarchy.dim_crm_opp_owner_region_stamped_id,
        fy23_and_beyond_user_hierarchy.crm_opp_owner_region_stamped,
        fy23_and_beyond_user_hierarchy.dim_crm_opp_owner_area_stamped_id,
        fy23_and_beyond_user_hierarchy.crm_opp_owner_area_stamped
    from target_matrix
    left join
        fy23_and_beyond_user_hierarchy
        on {{ sales_funnel_text_slugify("target_matrix.area") }}
        =
        {{
            sales_funnel_text_slugify(
                "fy23_and_beyond_user_hierarchy.crm_opp_owner_sales_segment_geo_region_area_stamped"
            )
        }}
        and target_matrix.fiscal_year = fy23_and_beyond_user_hierarchy.fiscal_year
    where target_matrix.fiscal_year > 2022

),
final_targets as (

    select

        {{
            dbt_utils.surrogate_key(
                [
                    "unioned_targets.crm_opp_owner_sales_segment_geo_region_area_stamped",
                    "unioned_targets.fiscal_year",
                    "unioned_targets.kpi_name",
                    "unioned_targets.first_day_of_month",
                    "unioned_targets.sales_qualified_source",
                    "unioned_targets.order_type",
                    "unioned_targets.dim_channel_type_id",
                    "unioned_targets.dim_alliance_type_id",
                ]
            )
        }}
        as sales_funnel_partner_alliance_target_id,
        unioned_targets.kpi_name,
        unioned_targets.first_day_of_month,
        unioned_targets.fiscal_year,
        unioned_targets.sales_qualified_source,
        unioned_targets.dim_sales_qualified_source_id,
        unioned_targets.alliance_partner as alliance_type,
        unioned_targets.dim_alliance_type_id,
        unioned_targets.order_type,
        unioned_targets.dim_order_type_id,
        unioned_targets.channel_type,
        unioned_targets.dim_channel_type_id,
        unioned_targets.crm_opp_owner_sales_segment_geo_region_area_stamped
        as crm_user_sales_segment_geo_region_area,
        coalesce(
            sfdc_user_hierarchy_live.dim_crm_user_hierarchy_live_id,
            unioned_targets.dim_crm_user_hierarchy_stamped_id
        ) as dim_crm_user_hierarchy_live_id,
        coalesce(
            sfdc_user_hierarchy_live.dim_crm_user_sales_segment_id,
            unioned_targets.dim_crm_opp_owner_sales_segment_stamped_id
        ) as dim_crm_user_sales_segment_id,
        coalesce(
            sfdc_user_hierarchy_live.dim_crm_user_geo_id,
            unioned_targets.dim_crm_opp_owner_geo_stamped_id
        ) as dim_crm_user_geo_id,
        coalesce(
            sfdc_user_hierarchy_live.dim_crm_user_region_id,
            unioned_targets.dim_crm_opp_owner_region_stamped_id
        ) as dim_crm_user_region_id,
        coalesce(
            sfdc_user_hierarchy_live.dim_crm_user_area_id,
            unioned_targets.dim_crm_opp_owner_area_stamped_id
        ) as dim_crm_user_area_id,
        unioned_targets.dim_crm_user_hierarchy_stamped_id,
        unioned_targets.dim_crm_opp_owner_sales_segment_stamped_id,
        unioned_targets.dim_crm_opp_owner_geo_stamped_id,
        unioned_targets.dim_crm_opp_owner_region_stamped_id,
        unioned_targets.dim_crm_opp_owner_area_stamped_id,
        sum(unioned_targets.allocated_target) as allocated_target

    from unioned_targets
    left join
        sfdc_user_hierarchy_live
        on unioned_targets.dim_crm_user_hierarchy_stamped_id
        = sfdc_user_hierarchy_live.dim_crm_user_hierarchy_live_id
        {{ dbt_utils.group_by(n=23) }}

)

{{
    dbt_audit(
        cte_ref="final_targets",
        created_by="@jpeguero",
        updated_by="@michellecooper",
        created_date="2021-04-08",
        updated_date="2022-03-07",
    )
}}
