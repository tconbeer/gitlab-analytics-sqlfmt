{{ config({
        "schema": "common"
    })
}}

WITH date AS (

   SELECT DISTINCT
     fiscal_month_name_fy,
     first_day_of_month
   FROM {{ ref('date_details_source') }}

), opportunity_source AS (

    SELECT *
    FROM {{ ref('dim_opportunity_source') }}

), order_type AS (

    SELECT *
    FROM {{ ref('dim_order_type') }}

), sfdc_user_hierarchy AS (

    SELECT *
    FROM {{ ref('prep_sales_hierarchy') }}

), target_matrix AS (

    SELECT *
    FROM {{ ref('sheetload_sales_funnel_targets_matrix_source' )}}

), final_targets AS (

  SELECT

    target_matrix.kpi_name,
    date.first_day_of_month,
    target_matrix.opportunity_source,
    opportunity_source.dim_opportunity_source_id,
    target_matrix.order_type,
    order_type.dim_order_type_id,
    sfdc_user_hierarchy.dim_sales_hierarchy_id,
    sfdc_user_hierarchy.dim_sales_segment_id,
    sfdc_user_hierarchy.dim_location_region_id,
    sfdc_user_hierarchy.dim_sales_region_id,
    sfdc_user_hierarchy.dim_sales_area_id,
    target_matrix.allocated_target,
    target_matrix.kpi_total,
    target_matrix.month_percentage,
    target_matrix.opportunity_source_percentage,
    target_matrix.order_type_percentage,
    target_matrix.area_percentage

  FROM target_matrix
  LEFT JOIN sfdc_user_hierarchy
    ON LOWER(target_matrix.area) = LOWER(sfdc_user_hierarchy.user_area)
  LEFT JOIN date
    ON target_matrix.month = date.fiscal_month_name_fy
  LEFT JOIN opportunity_source
    ON target_matrix.opportunity_source = opportunity_source.opportunity_source_name
  LEFT JOIN order_type
    ON target_matrix.order_type = order_type.order_type_name
)

{{ dbt_audit(
    cte_ref="final_targets",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-12-18",
    updated_date="2020-12-18"
) }}
