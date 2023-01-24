{{
    simple_cte(
        [
            ("crm_account_dimensions", "map_crm_account"),
            ("prep_crm_account", "prep_crm_account"),
            ("order_type", "prep_order_type"),
            ("sales_qualified_source", "prep_sales_qualified_source"),
            ("deal_path", "prep_deal_path"),
            ("sales_rep", "prep_crm_user"),
            ("sales_segment", "prep_sales_segment"),
            ("sfdc_campaigns", "prep_campaign"),
            ("dr_partner_engagement", "prep_dr_partner_engagement"),
            ("alliance_type", "prep_alliance_type"),
            ("channel_type", "prep_channel_type"),
            ("sfdc_opportunity", "prep_crm_opportunity"),
        ]
    )
}},
user_hierarchy_stamped_sales_segment as (

    select distinct
        dim_crm_opp_owner_sales_segment_stamped_id, crm_opp_owner_sales_segment_stamped
    from {{ ref("prep_crm_user_hierarchy_stamped") }}

),
user_hierarchy_stamped_geo as (

    select distinct dim_crm_opp_owner_geo_stamped_id, crm_opp_owner_geo_stamped
    from {{ ref("prep_crm_user_hierarchy_stamped") }}

),
user_hierarchy_stamped_region as (

    select distinct dim_crm_opp_owner_region_stamped_id, crm_opp_owner_region_stamped
    from {{ ref("prep_crm_user_hierarchy_stamped") }}

),
user_hierarchy_stamped_area as (

    select distinct dim_crm_opp_owner_area_stamped_id, crm_opp_owner_area_stamped
    from {{ ref("prep_crm_user_hierarchy_stamped") }}

),
final_opportunities as (

    select

        -- opportunity and person ids
        sfdc_opportunity.dim_crm_opportunity_id,
        sfdc_opportunity.merged_opportunity_id as merged_crm_opportunity_id,
        sfdc_opportunity.dim_crm_account_id,
        crm_account_dimensions.dim_parent_crm_account_id,
        sfdc_opportunity.dim_crm_person_id,
        sfdc_opportunity.sfdc_contact_id,

        -- dates
        sfdc_opportunity.created_date,
        sfdc_opportunity.created_date_id,
        sfdc_opportunity.sales_accepted_date,
        sfdc_opportunity.sales_accepted_date_id,
        sfdc_opportunity.close_date,
        sfdc_opportunity.close_date_id,
        sfdc_opportunity.stage_0_pending_acceptance_date,
        sfdc_opportunity.stage_0_pending_acceptance_date_id,
        sfdc_opportunity.stage_1_discovery_date,
        sfdc_opportunity.stage_1_discovery_date_id,
        sfdc_opportunity.stage_2_scoping_date,
        sfdc_opportunity.stage_2_scoping_date_id,
        sfdc_opportunity.stage_3_technical_evaluation_date,
        sfdc_opportunity.stage_3_technical_evaluation_date_id,
        sfdc_opportunity.stage_4_proposal_date,
        sfdc_opportunity.stage_4_proposal_date_id,
        sfdc_opportunity.stage_5_negotiating_date,
        sfdc_opportunity.stage_5_negotiating_date_id,
        sfdc_opportunity.stage_6_closed_won_date,
        sfdc_opportunity.stage_6_closed_won_date_id,
        sfdc_opportunity.stage_6_closed_lost_date,
        sfdc_opportunity.stage_6_closed_lost_date_id,
        sfdc_opportunity.days_in_0_pending_acceptance,
        sfdc_opportunity.days_in_1_discovery,
        sfdc_opportunity.days_in_2_scoping,
        sfdc_opportunity.days_in_3_technical_evaluation,
        sfdc_opportunity.days_in_4_proposal,
        sfdc_opportunity.days_in_5_negotiating,
        sfdc_opportunity.days_in_sao,
        sfdc_opportunity.closed_buckets,
        sfdc_opportunity.subscription_start_date,
        sfdc_opportunity.subscription_end_date,

        -- common dimension keys
        {{ get_keyed_nulls("sfdc_opportunity.dim_crm_user_id") }} as dim_crm_user_id,
        {{ get_keyed_nulls("prep_crm_account.dim_crm_user_id") }}
        as dim_crm_account_user_id,
        {{ get_keyed_nulls("order_type.dim_order_type_id") }} as dim_order_type_id,
        {{ get_keyed_nulls("dr_partner_engagement.dim_dr_partner_engagement_id") }}
        as dim_dr_partner_engagement_id,
        {{ get_keyed_nulls("alliance_type.dim_alliance_type_id") }}
        as dim_alliance_type_id,
        {{ get_keyed_nulls("channel_type.dim_channel_type_id") }}
        as dim_channel_type_id,
        {{ get_keyed_nulls("sales_qualified_source.dim_sales_qualified_source_id") }}
        as dim_sales_qualified_source_id,
        {{ get_keyed_nulls("deal_path.dim_deal_path_id") }} as dim_deal_path_id,
        {{
            get_keyed_nulls(
                "crm_account_dimensions.dim_parent_sales_segment_id,sales_segment.dim_sales_segment_id"
            )
        }}
        as dim_parent_sales_segment_id,
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
        crm_account_dimensions.dim_account_location_region_id,
        {{
            get_keyed_nulls(
                "user_hierarchy_stamped_sales_segment.dim_crm_opp_owner_sales_segment_stamped_id"
            )
        }}
        as dim_crm_opp_owner_sales_segment_stamped_id,
        {{
            get_keyed_nulls(
                "user_hierarchy_stamped_geo.dim_crm_opp_owner_geo_stamped_id"
            )
        }} as dim_crm_opp_owner_geo_stamped_id,
        {{
            get_keyed_nulls(
                "user_hierarchy_stamped_region.dim_crm_opp_owner_region_stamped_id"
            )
        }} as dim_crm_opp_owner_region_stamped_id,
        {{
            get_keyed_nulls(
                "user_hierarchy_stamped_area.dim_crm_opp_owner_area_stamped_id"
            )
        }} as dim_crm_opp_owner_area_stamped_id,
        {{ get_keyed_nulls("sales_rep.dim_crm_user_sales_segment_id") }}
        as dim_crm_user_sales_segment_id,
        {{ get_keyed_nulls("sales_rep.dim_crm_user_geo_id") }} as dim_crm_user_geo_id,
        {{ get_keyed_nulls("sales_rep.dim_crm_user_region_id") }}
        as dim_crm_user_region_id,
        {{ get_keyed_nulls("sales_rep.dim_crm_user_area_id") }} as dim_crm_user_area_id,
        {{ get_keyed_nulls("sales_rep_account.dim_crm_user_sales_segment_id") }}
        as dim_crm_account_user_sales_segment_id,
        {{ get_keyed_nulls("sales_rep_account.dim_crm_user_geo_id") }}
        as dim_crm_account_user_geo_id,
        {{ get_keyed_nulls("sales_rep_account.dim_crm_user_region_id") }}
        as dim_crm_account_user_region_id,
        {{ get_keyed_nulls("sales_rep_account.dim_crm_user_area_id") }}
        as dim_crm_account_user_area_id,

        -- flags
        sfdc_opportunity.is_closed,
        sfdc_opportunity.is_won,
        sfdc_opportunity.is_refund,
        sfdc_opportunity.is_downgrade,
        sfdc_opportunity.is_swing_deal,
        sfdc_opportunity.is_edu_oss,
        sfdc_opportunity.is_web_portal_purchase,
        sfdc_opportunity.fpa_master_bookings_flag,
        sfdc_opportunity.is_sao,
        sfdc_opportunity.is_sdr_sao,
        sfdc_opportunity.is_net_arr_closed_deal,
        sfdc_opportunity.is_new_logo_first_order,
        sfdc_opportunity.is_net_arr_pipeline_created,
        sfdc_opportunity.is_win_rate_calc,
        sfdc_opportunity.is_closed_won,

        sfdc_opportunity.primary_solution_architect,
        sfdc_opportunity.product_details,
        sfdc_opportunity.product_category,
        sfdc_opportunity.products_purchased,
        sfdc_opportunity.growth_type,
        sfdc_opportunity.opportunity_deal_size,

        -- channel fields
        sfdc_opportunity.lead_source,
        sfdc_opportunity.dr_partner_deal_type,
        sfdc_opportunity.dr_partner_engagement,
        sfdc_opportunity.partner_account,
        sfdc_opportunity.dr_status,
        sfdc_opportunity.dr_deal_id,
        sfdc_opportunity.dr_primary_registration,
        sfdc_opportunity.distributor,
        sfdc_opportunity.influence_partner,
        sfdc_opportunity.fulfillment_partner,
        sfdc_opportunity.platform_partner,
        sfdc_opportunity.partner_track,
        sfdc_opportunity.is_public_sector_opp,
        sfdc_opportunity.is_registration_from_portal,
        sfdc_opportunity.calculated_discount,
        sfdc_opportunity.partner_discount,
        sfdc_opportunity.partner_discount_calc,
        sfdc_opportunity.comp_channel_neutral,

        -- additive fields
        sfdc_opportunity.incremental_acv as iacv,
        sfdc_opportunity.net_incremental_acv as net_iacv,
        sfdc_opportunity.net_arr,
        sfdc_opportunity.new_logo_count,
        sfdc_opportunity.amount,
        sfdc_opportunity.recurring_amount,
        sfdc_opportunity.true_up_amount,
        sfdc_opportunity.proserv_amount,
        sfdc_opportunity.other_non_recurring_amount,
        sfdc_opportunity.arr_basis,
        sfdc_opportunity.arr,
        sfdc_opportunity.count_crm_attribution_touchpoints,
        sfdc_opportunity.weighted_linear_iacv,
        sfdc_opportunity.count_campaigns,
        sfdc_opportunity.probability

    from sfdc_opportunity
    left join
        crm_account_dimensions
        on sfdc_opportunity.dim_crm_account_id
        = crm_account_dimensions.dim_crm_account_id
    left join
        prep_crm_account
        on sfdc_opportunity.dim_crm_account_id = prep_crm_account.dim_crm_account_id
    left join
        sales_qualified_source
        on sfdc_opportunity.sales_qualified_source
        = sales_qualified_source.sales_qualified_source_name
    left join order_type on sfdc_opportunity.order_type = order_type.order_type_name
    left join deal_path on sfdc_opportunity.deal_path = deal_path.deal_path_name
    left join
        sales_segment
        on sfdc_opportunity.sales_segment = sales_segment.sales_segment_name
    left join
        user_hierarchy_stamped_sales_segment
        on sfdc_opportunity.crm_opp_owner_sales_segment_stamped
        = user_hierarchy_stamped_sales_segment.crm_opp_owner_sales_segment_stamped
    left join
        user_hierarchy_stamped_geo
        on sfdc_opportunity.crm_opp_owner_geo_stamped
        = user_hierarchy_stamped_geo.crm_opp_owner_geo_stamped
    left join
        user_hierarchy_stamped_region
        on sfdc_opportunity.crm_opp_owner_region_stamped
        = user_hierarchy_stamped_region.crm_opp_owner_region_stamped
    left join
        user_hierarchy_stamped_area
        on sfdc_opportunity.crm_opp_owner_area_stamped
        = user_hierarchy_stamped_area.crm_opp_owner_area_stamped
    left join
        dr_partner_engagement
        on sfdc_opportunity.dr_partner_engagement
        = dr_partner_engagement.dr_partner_engagement_name
    left join
        alliance_type
        on sfdc_opportunity.alliance_type = alliance_type.alliance_type_name
    left join
        channel_type on sfdc_opportunity.channel_type = channel_type.channel_type_name
    left join sales_rep on sfdc_opportunity.dim_crm_user_id = sales_rep.dim_crm_user_id
    left join
        sales_rep as sales_rep_account
        on prep_crm_account.dim_crm_user_id = sales_rep_account.dim_crm_user_id

)

{{
    dbt_audit(
        cte_ref="final_opportunities",
        created_by="@mcooperDD",
        updated_by="@michellecooper",
        created_date="2020-11-30",
        updated_date="2022-03-17",
    )
}}
