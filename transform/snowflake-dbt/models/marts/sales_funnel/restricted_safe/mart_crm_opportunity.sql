{{
    simple_cte(
        [
            ("dim_crm_account", "dim_crm_account"),
            ("dim_crm_opportunity", "dim_crm_opportunity"),
            ("dim_sales_qualified_source", "dim_sales_qualified_source"),
            ("dim_order_type", "dim_order_type"),
            ("dim_deal_path", "dim_deal_path"),
            ("fct_crm_opportunity", "fct_crm_opportunity"),
            ("dim_dr_partner_engagement", "dim_dr_partner_engagement"),
            ("dim_alliance_type", "dim_alliance_type"),
            ("dim_channel_type", "dim_channel_type"),
        ]
    )
}}

,
dim_crm_user_hierarchy_live_sales_segment as (

    select distinct
        dim_crm_user_sales_segment_id,
        crm_user_sales_segment,
        crm_user_sales_segment_grouped
    from {{ ref("dim_crm_user_hierarchy_live") }}

),
dim_crm_user_hierarchy_live_geo as (

    select distinct dim_crm_user_geo_id, crm_user_geo
    from {{ ref("dim_crm_user_hierarchy_live") }}

),
dim_crm_user_hierarchy_live_region as (

    select distinct dim_crm_user_region_id, crm_user_region
    from {{ ref("dim_crm_user_hierarchy_live") }}

),
dim_crm_user_hierarchy_live_area as (

    select distinct dim_crm_user_area_id, crm_user_area
    from {{ ref("dim_crm_user_hierarchy_live") }}

),
dim_crm_user_hierarchy_stamped_sales_segment as (

    select distinct
        dim_crm_opp_owner_sales_segment_stamped_id,
        crm_opp_owner_sales_segment_stamped,
        crm_opp_owner_sales_segment_stamped_grouped
    from {{ ref("dim_crm_user_hierarchy_stamped") }}

),
dim_crm_user_hierarchy_stamped_geo as (

    select distinct dim_crm_opp_owner_geo_stamped_id, crm_opp_owner_geo_stamped
    from {{ ref("dim_crm_user_hierarchy_stamped") }}

),
dim_crm_user_hierarchy_stamped_region as (

    select distinct dim_crm_opp_owner_region_stamped_id, crm_opp_owner_region_stamped
    from {{ ref("dim_crm_user_hierarchy_stamped") }}

),
dim_crm_user_hierarchy_stamped_area as (

    select distinct dim_crm_opp_owner_area_stamped_id, crm_opp_owner_area_stamped
    from {{ ref("dim_crm_user_hierarchy_stamped") }}

),
final as (

    select
        fct_crm_opportunity.sales_accepted_date,
        date_trunc(
            month, fct_crm_opportunity.sales_accepted_date
        ) as sales_accepted_month,
        fct_crm_opportunity.close_date,
        date_trunc(month, fct_crm_opportunity.close_date) as close_month,
        fct_crm_opportunity.created_date,
        date_trunc(month, fct_crm_opportunity.created_date) as created_month,
        fct_crm_opportunity.dim_crm_opportunity_id,
        dim_crm_opportunity.opportunity_name,
        dim_crm_account.parent_crm_account_name,
        dim_crm_account.dim_parent_crm_account_id,
        dim_crm_account.crm_account_name,
        dim_crm_account.dim_crm_account_id,
        dim_crm_opportunity.dim_crm_user_id as dim_crm_sales_rep_id,

        -- opportunity attributes & additive fields
        fct_crm_opportunity.is_won,
        fct_crm_opportunity.is_closed,
        fct_crm_opportunity.days_in_sao,
        fct_crm_opportunity.arr_basis,
        fct_crm_opportunity.iacv,
        fct_crm_opportunity.net_iacv,
        fct_crm_opportunity.net_arr,
        fct_crm_opportunity.new_logo_count,
        fct_crm_opportunity.amount,
        dim_crm_opportunity.is_edu_oss,
        dim_crm_opportunity.is_ps_opp,
        dim_crm_opportunity.stage_name,
        dim_crm_opportunity.reason_for_loss,
        dim_crm_opportunity.sales_type,
        fct_crm_opportunity.is_sao,
        fct_crm_opportunity.is_net_arr_closed_deal,
        fct_crm_opportunity.is_new_logo_first_order,
        fct_crm_opportunity.is_net_arr_pipeline_created,
        fct_crm_opportunity.is_win_rate_calc,
        fct_crm_opportunity.is_closed_won,
        dim_deal_path.deal_path_name,
        dim_order_type.order_type_name as order_type,
        dim_order_type.order_type_grouped,
        dim_dr_partner_engagement.dr_partner_engagement_name,
        dim_alliance_type.alliance_type_name,
        dim_alliance_type.alliance_type_short_name,
        dim_channel_type.channel_type_name,
        dim_sales_qualified_source.sales_qualified_source_name,
        dim_sales_qualified_source.sales_qualified_source_grouped,
        dim_sales_qualified_source.sqs_bucket_engagement,
        dim_crm_account.is_jihu_account,
        dim_crm_account.fy22_new_logo_target_list,
        dim_crm_account.crm_account_gtm_strategy,
        dim_crm_account.crm_account_focus_account,
        dim_crm_account.crm_account_zi_technologies,
        dim_crm_account.parent_crm_account_gtm_strategy,
        dim_crm_account.parent_crm_account_focus_account,
        dim_crm_account.parent_crm_account_sales_segment,
        dim_crm_account.parent_crm_account_zi_technologies,
        dim_crm_account.parent_crm_account_demographics_sales_segment,
        dim_crm_account.parent_crm_account_demographics_geo,
        dim_crm_account.parent_crm_account_demographics_region,
        dim_crm_account.parent_crm_account_demographics_area,
        dim_crm_account.parent_crm_account_demographics_territory,
        dim_crm_account.crm_account_demographics_employee_count,
        dim_crm_account.parent_crm_account_demographics_max_family_employee,
        dim_crm_account.parent_crm_account_demographics_upa_country,
        dim_crm_account.parent_crm_account_demographics_upa_state,
        dim_crm_account.parent_crm_account_demographics_upa_city,
        dim_crm_account.parent_crm_account_demographics_upa_street,
        dim_crm_account.parent_crm_account_demographics_upa_postal_code,
        fct_crm_opportunity.closed_buckets,
        dim_crm_opportunity.duplicate_opportunity_id,
        dim_crm_opportunity.opportunity_category,
        dim_crm_opportunity.source_buckets,
        dim_crm_opportunity.opportunity_sales_development_representative,
        dim_crm_opportunity.opportunity_business_development_representative,
        dim_crm_opportunity.opportunity_development_representative,
        dim_crm_opportunity.sdr_or_bdr,
        dim_crm_opportunity.iqm_submitted_by_role,
        dim_crm_opportunity.sdr_pipeline_contribution,
        dim_crm_opportunity.is_web_portal_purchase,
        fct_crm_opportunity.fpa_master_bookings_flag,
        dim_crm_opportunity.sales_path,
        dim_crm_opportunity.professional_services_value,
        fct_crm_opportunity.primary_solution_architect,
        fct_crm_opportunity.product_details,
        fct_crm_opportunity.product_category,
        fct_crm_opportunity.products_purchased,
        fct_crm_opportunity.growth_type,
        fct_crm_opportunity.opportunity_deal_size,
        dim_crm_opportunity.primary_campaign_source_id,

        -- crm opp owner/account owner fields stamped at SAO date
        dim_crm_opportunity.sao_crm_opp_owner_stamped_name,
        dim_crm_opportunity.sao_crm_account_owner_stamped_name,
        dim_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped,
        dim_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped_grouped,
        dim_crm_opportunity.sao_crm_opp_owner_geo_stamped,
        dim_crm_opportunity.sao_crm_opp_owner_region_stamped,
        dim_crm_opportunity.sao_crm_opp_owner_area_stamped,
        dim_crm_opportunity.sao_crm_opp_owner_segment_region_stamped_grouped,
        dim_crm_opportunity.sao_crm_opp_owner_sales_segment_geo_region_area_stamped,

        -- crm opp owner/account owner stamped fields stamped at close date
        dim_crm_opportunity.crm_opp_owner_stamped_name,
        dim_crm_opportunity.crm_account_owner_stamped_name,
        dim_crm_user_hierarchy_stamped_sales_segment.crm_opp_owner_sales_segment_stamped,
        dim_crm_user_hierarchy_stamped_sales_segment.crm_opp_owner_sales_segment_stamped_grouped,
        dim_crm_user_hierarchy_stamped_geo.crm_opp_owner_geo_stamped,
        dim_crm_user_hierarchy_stamped_region.crm_opp_owner_region_stamped,
        dim_crm_user_hierarchy_stamped_area.crm_opp_owner_area_stamped,
        {{
            sales_segment_region_grouped(
                "dim_crm_user_hierarchy_stamped_sales_segment.crm_opp_owner_sales_segment_stamped",
                "dim_crm_user_hierarchy_stamped_geo.crm_opp_owner_geo_stamped",
                "dim_crm_user_hierarchy_stamped_region.crm_opp_owner_region_stamped",
            )
        }}
        as crm_opp_owner_sales_segment_region_stamped_grouped,
        dim_crm_opportunity.crm_opp_owner_sales_segment_geo_region_area_stamped,
        dim_crm_opportunity.crm_opp_owner_user_role_type_stamped,

        -- crm owner/sales rep live fields
        dim_crm_user_hierarchy_live_sales_segment.crm_user_sales_segment,
        dim_crm_user_hierarchy_live_sales_segment.crm_user_sales_segment_grouped,
        dim_crm_user_hierarchy_live_geo.crm_user_geo,
        dim_crm_user_hierarchy_live_region.crm_user_region,
        dim_crm_user_hierarchy_live_area.crm_user_area,
        {{
            sales_segment_region_grouped(
                "dim_crm_user_hierarchy_live_sales_segment.crm_user_sales_segment",
                "dim_crm_user_hierarchy_live_geo.crm_user_geo",
                "dim_crm_user_hierarchy_live_region.crm_user_region",
            )
        }} as crm_user_sales_segment_region_grouped,


        -- crm account owner/sales rep live fields
        dim_crm_account_user_hierarchy_live_sales_segment.crm_user_sales_segment
        as crm_account_user_sales_segment,
        dim_crm_account_user_hierarchy_live_sales_segment.crm_user_sales_segment_grouped
        as crm_account_user_sales_segment_grouped,
        dim_crm_account_user_hierarchy_live_geo.crm_user_geo as crm_account_user_geo,
        dim_crm_account_user_hierarchy_live_region.crm_user_region
        as crm_account_user_region,
        dim_crm_account_user_hierarchy_live_area.crm_user_area as crm_account_user_area,
        {{
            sales_segment_region_grouped(
                "dim_crm_account_user_hierarchy_live_sales_segment.crm_user_sales_segment",
                "dim_crm_account_user_hierarchy_live_geo.crm_user_geo",
                "dim_crm_account_user_hierarchy_live_region.crm_user_region",
            )
        }}
        as crm_account_user_sales_segment_region_grouped,

        -- channel fields
        fct_crm_opportunity.lead_source,
        fct_crm_opportunity.dr_partner_deal_type,
        fct_crm_opportunity.partner_account,
        fct_crm_opportunity.dr_status,
        fct_crm_opportunity.distributor,
        fct_crm_opportunity.dr_deal_id,
        fct_crm_opportunity.dr_primary_registration,
        fct_crm_opportunity.influence_partner,
        fct_crm_opportunity.fulfillment_partner,
        fct_crm_opportunity.platform_partner,
        fct_crm_opportunity.partner_track,
        fct_crm_opportunity.is_public_sector_opp,
        fct_crm_opportunity.is_registration_from_portal,
        fct_crm_opportunity.calculated_discount,
        fct_crm_opportunity.partner_discount,
        fct_crm_opportunity.partner_discount_calc,
        fct_crm_opportunity.comp_channel_neutral,
        fct_crm_opportunity.count_crm_attribution_touchpoints,
        fct_crm_opportunity.weighted_linear_iacv,
        fct_crm_opportunity.count_campaigns,

        -- Solutions-Architech fields
        dim_crm_opportunity.sa_tech_evaluation_close_status,
        dim_crm_opportunity.sa_tech_evaluation_end_date,
        dim_crm_opportunity.sa_tech_evaluation_start_date,


        -- Command Plan fields
        dim_crm_opportunity.cp_partner,
        dim_crm_opportunity.cp_paper_process,
        dim_crm_opportunity.cp_help,
        dim_crm_opportunity.cp_review_notes

    from fct_crm_opportunity
    left join
        dim_crm_opportunity
        on fct_crm_opportunity.dim_crm_opportunity_id
        = dim_crm_opportunity.dim_crm_opportunity_id
    left join
        dim_crm_account
        on dim_crm_opportunity.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    left join
        dim_sales_qualified_source
        on fct_crm_opportunity.dim_sales_qualified_source_id
        = dim_sales_qualified_source.dim_sales_qualified_source_id
    left join
        dim_deal_path
        on fct_crm_opportunity.dim_deal_path_id = dim_deal_path.dim_deal_path_id
    left join
        dim_order_type
        on fct_crm_opportunity.dim_order_type_id = dim_order_type.dim_order_type_id
    left join
        dim_dr_partner_engagement
        on fct_crm_opportunity.dim_dr_partner_engagement_id
        = dim_dr_partner_engagement.dim_dr_partner_engagement_id
    left join
        dim_alliance_type
        on fct_crm_opportunity.dim_alliance_type_id
        = dim_alliance_type.dim_alliance_type_id
    left join
        dim_channel_type
        on fct_crm_opportunity.dim_channel_type_id
        = dim_channel_type.dim_channel_type_id
    left join
        dim_crm_user_hierarchy_stamped_sales_segment
        on fct_crm_opportunity.dim_crm_opp_owner_sales_segment_stamped_id
        = dim_crm_user_hierarchy_stamped_sales_segment.dim_crm_opp_owner_sales_segment_stamped_id
    left join
        dim_crm_user_hierarchy_stamped_geo
        on fct_crm_opportunity.dim_crm_opp_owner_geo_stamped_id
        = dim_crm_user_hierarchy_stamped_geo.dim_crm_opp_owner_geo_stamped_id
    left join
        dim_crm_user_hierarchy_stamped_region
        on fct_crm_opportunity.dim_crm_opp_owner_region_stamped_id
        = dim_crm_user_hierarchy_stamped_region.dim_crm_opp_owner_region_stamped_id
    left join
        dim_crm_user_hierarchy_stamped_area
        on fct_crm_opportunity.dim_crm_opp_owner_area_stamped_id
        = dim_crm_user_hierarchy_stamped_area.dim_crm_opp_owner_area_stamped_id
    left join
        dim_crm_user_hierarchy_live_sales_segment
        on fct_crm_opportunity.dim_crm_user_sales_segment_id
        = dim_crm_user_hierarchy_live_sales_segment.dim_crm_user_sales_segment_id
    left join
        dim_crm_user_hierarchy_live_geo
        on fct_crm_opportunity.dim_crm_user_geo_id
        = dim_crm_user_hierarchy_live_geo.dim_crm_user_geo_id
    left join
        dim_crm_user_hierarchy_live_region
        on fct_crm_opportunity.dim_crm_user_region_id
        = dim_crm_user_hierarchy_live_region.dim_crm_user_region_id
    left join
        dim_crm_user_hierarchy_live_area
        on fct_crm_opportunity.dim_crm_user_area_id
        = dim_crm_user_hierarchy_live_area.dim_crm_user_area_id
    left join
        dim_crm_user_hierarchy_live_sales_segment
        as dim_crm_account_user_hierarchy_live_sales_segment
        on fct_crm_opportunity.dim_crm_account_user_sales_segment_id
        = dim_crm_account_user_hierarchy_live_sales_segment.dim_crm_user_sales_segment_id
    left join
        dim_crm_user_hierarchy_live_geo as dim_crm_account_user_hierarchy_live_geo
        on fct_crm_opportunity.dim_crm_account_user_geo_id
        = dim_crm_account_user_hierarchy_live_geo.dim_crm_user_geo_id
    left join
        dim_crm_user_hierarchy_live_region as dim_crm_account_user_hierarchy_live_region
        on fct_crm_opportunity.dim_crm_account_user_region_id
        = dim_crm_account_user_hierarchy_live_region.dim_crm_user_region_id
    left join
        dim_crm_user_hierarchy_live_area as dim_crm_account_user_hierarchy_live_area
        on fct_crm_opportunity.dim_crm_account_user_area_id
        = dim_crm_account_user_hierarchy_live_area.dim_crm_user_area_id

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@iweeks",
        updated_by="@rkohnke",
        created_date="2020-12-07",
        updated_date="2022-04-26",
    )
}}
