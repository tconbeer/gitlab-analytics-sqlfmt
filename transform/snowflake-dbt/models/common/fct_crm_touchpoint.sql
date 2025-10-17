{{ config(tags=["mnpi_exception"]) }}

with
    account_dimensions as (select * from {{ ref("map_crm_account") }}),
    bizible_touchpoints as (

        select *
        from {{ ref("sfdc_bizible_touchpoint_source") }}
        where is_deleted = 'FALSE'

    ),
    crm_person as (select * from {{ ref("prep_crm_person") }}),
    final_touchpoint as (

        select
            touchpoint_id as dim_crm_touchpoint_id,
            bizible_touchpoints.bizible_person_id,

            -- shared dimension keys
            crm_person.dim_crm_person_id,
            crm_person.dim_crm_user_id,
            campaign_id as dim_campaign_id,
            account_dimensions.dim_crm_account_id,
            account_dimensions.dim_parent_crm_account_id,
            account_dimensions.dim_parent_sales_segment_id,
            account_dimensions.dim_parent_sales_territory_id,
            account_dimensions.dim_parent_industry_id,
            account_dimensions.dim_parent_location_country_id,
            account_dimensions.dim_parent_location_region_id,
            account_dimensions.dim_account_sales_segment_id,
            account_dimensions.dim_account_sales_territory_id,
            account_dimensions.dim_account_industry_id,
            account_dimensions.dim_account_location_country_id,
            account_dimensions.dim_account_location_region_id,

            -- attribution counts
            bizible_count_first_touch,
            bizible_count_lead_creation_touch,
            bizible_count_u_shaped

        from bizible_touchpoints
        left join
            account_dimensions
            on bizible_touchpoints.bizible_account
            = account_dimensions.dim_crm_account_id
        left join
            crm_person
            on bizible_touchpoints.bizible_person_id = crm_person.bizible_person_id
    )

    {{
        dbt_audit(
            cte_ref="final_touchpoint",
            created_by="@mcooperDD",
            updated_by="@rkohnke",
            created_date="2021-01-21",
            updated_date="2021-10-05",
        )
    }}
