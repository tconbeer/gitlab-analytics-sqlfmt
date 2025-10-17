{{ config(tags=["mnpi_exception"]) }}

with
    bizible_attribution_touchpoints as (

        select *
        from {{ ref("sfdc_bizible_attribution_touchpoint_source") }}
        where is_deleted = 'FALSE'

    ),
    crm_person as (select * from {{ ref("prep_crm_person") }}),
    opportunity_dimensions as (select * from {{ ref("map_crm_opportunity") }}),
    final_attribution_touchpoint as (

        select
            touchpoint_id as dim_crm_touchpoint_id,

            -- shared dimension keys
            campaign_id as dim_campaign_id,
            opportunity_id as dim_crm_opportunity_id,
            bizible_account as dim_crm_account_id,
            crm_person.dim_crm_person_id,
            opportunity_dimensions.dim_crm_user_id,
            opportunity_dimensions.dim_order_type_id,
            opportunity_dimensions.dim_sales_qualified_source_id,
            opportunity_dimensions.dim_deal_path_id,
            opportunity_dimensions.dim_parent_crm_account_id,
            opportunity_dimensions.dim_parent_sales_segment_id,
            opportunity_dimensions.dim_parent_sales_territory_id,
            opportunity_dimensions.dim_parent_industry_id,
            opportunity_dimensions.dim_parent_location_country_id,
            opportunity_dimensions.dim_parent_location_region_id,
            opportunity_dimensions.dim_account_sales_segment_id,
            opportunity_dimensions.dim_account_sales_territory_id,
            opportunity_dimensions.dim_account_industry_id,
            opportunity_dimensions.dim_account_location_country_id,
            opportunity_dimensions.dim_account_location_region_id,

            -- attribution counts
            bizible_count_first_touch,
            bizible_count_lead_creation_touch,
            bizible_attribution_percent_full_path,
            bizible_count_u_shaped,
            bizible_count_w_shaped,
            bizible_count_custom_model,

            -- touchpoint revenue info
            bizible_revenue_full_path,
            bizible_revenue_custom_model,
            bizible_revenue_first_touch,
            bizible_revenue_lead_conversion,
            bizible_revenue_u_shaped,
            bizible_revenue_w_shaped

        from bizible_attribution_touchpoints
        left join
            crm_person
            on bizible_attribution_touchpoints.bizible_contact
            = crm_person.sfdc_record_id
        left join
            opportunity_dimensions
            on bizible_attribution_touchpoints.opportunity_id
            = opportunity_dimensions.dim_crm_opportunity_id
    )

    {{
        dbt_audit(
            cte_ref="final_attribution_touchpoint",
            created_by="@mcooperDD",
            updated_by="@michellecooper",
            created_date="2021-01-21",
            updated_date="2021-11-02",
        )
    }}
