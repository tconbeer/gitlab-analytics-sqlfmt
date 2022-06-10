{{ config(tags=["mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("dim_date", "dim_date"),
            ("sfdc_user_snapshots_source", "sfdc_user_snapshots_source"),
            ("sfdc_opportunity_source", "sfdc_opportunity_source"),
        ]
    )
}}

,
sheetload_sales_funnel_targets_matrix_source as (

    select
        sheetload_sales_funnel_targets_matrix_source.*,
        concat(
            sheetload_sales_funnel_targets_matrix_source.user_segment,
            '-',
            sheetload_sales_funnel_targets_matrix_source.user_geo,
            '-',
            sheetload_sales_funnel_targets_matrix_source.user_region,
            '-',
            sheetload_sales_funnel_targets_matrix_source.user_area
        ) as user_segment_geo_region_area
    from {{ ref("sheetload_sales_funnel_targets_matrix_source") }}

),
fiscal_months as (

    select distinct fiscal_month_name_fy, fiscal_year, first_day_of_month from dim_date

),
base_scd as (
    /*
  Find the minimum valid from and valid to dates for each combo of segment-geo-region-area
*/
    select
        user_segment,
        user_geo,
        user_region,
        user_area,
        coalesce(
            user_segment_geo_region_area,
            concat(
                ifnull(user_segment, 'No User Segment'),
                '-',
                ifnull(user_geo, 'No User Geo'),
                '-',
                ifnull(user_region, 'No User Region'),
                '-',
                ifnull(user_area, 'No user_area')
            )
        ) as user_segment_geo_region_area,
        min(dbt_valid_from) as valid_from,
        max(dbt_valid_to) as valid_to
    from sfdc_user_snapshots_source {{ dbt_utils.group_by(n=5) }}

),
base_scd_spined as (
    /*
  Expand the slowly changing dimension to the daily grain and add flags to indicate the last user hierarchy (segement-geo-region-area) in a fiscal year as well as the last user area (user_area)
  in a fiscal year. These will be used to join to the sales funnel target model. FY22 targets were set at the user_area level, while the FY23 targets (and beyond) will be set at the 
  segment-geo-region-area grain. 
*/
    select
        base_scd.*,
        dim_date.date_actual as snapshot_date,
        dim_date.fiscal_year,
        iff(
            row_number() over (
                partition by user_area, user_segment_geo_region_area, fiscal_year
                order by date_actual desc
            ) = 1,
            1,
            0
        ) as is_last_user_hierarchy_in_fiscal_year,
        iff(
            row_number() over (
                partition by user_area, fiscal_year
                order by valid_to desc, snapshot_date desc
            ) = 1,
            1,
            0
        ) as is_last_user_area_in_fiscal_year
    from base_scd
    inner join
        dim_date
        on dim_date.date_actual >= base_scd.valid_from
        and dim_date.date_actual < base_scd.valid_to
    where user_area is not null

),
final_scd as (

    select
        user_segment,
        user_geo,
        user_region,
        user_area,
        user_segment_geo_region_area,
        fiscal_year,
        is_last_user_hierarchy_in_fiscal_year,
        is_last_user_area_in_fiscal_year
    from base_scd_spined
    where
        is_last_user_hierarchy_in_fiscal_year = 1
        or is_last_user_area_in_fiscal_year = 1

),
user_hierarchy_sheetload as (
    /*
  To get a complete picture of the hierarchy and to ensure fidelity with the TOPO model, we will union in the distinct hierarchy values from the file.
*/
    select distinct
        fiscal_months.fiscal_year,
        sheetload_sales_funnel_targets_matrix_source.user_segment,
        sheetload_sales_funnel_targets_matrix_source.user_geo,
        sheetload_sales_funnel_targets_matrix_source.user_region,
        sheetload_sales_funnel_targets_matrix_source.user_area,
        sheetload_sales_funnel_targets_matrix_source.user_segment_geo_region_area,
        coalesce(
            final_scd.is_last_user_hierarchy_in_fiscal_year, 1
        ) as is_last_user_hierarchy_in_fiscal_year,
        coalesce(
            final_scd.is_last_user_area_in_fiscal_year, 0
        ) as is_last_user_area_in_fiscal_year
    from sheetload_sales_funnel_targets_matrix_source
    inner join
        fiscal_months
        on sheetload_sales_funnel_targets_matrix_source.month
        = fiscal_months.fiscal_month_name_fy
    left join
        final_scd on lower(
            sheetload_sales_funnel_targets_matrix_source.user_segment_geo_region_area
        ) = lower(
            final_scd.user_segment_geo_region_area
        ) and fiscal_months.fiscal_year = final_scd.fiscal_year
    where
        sheetload_sales_funnel_targets_matrix_source.user_area != 'N/A'
        and sheetload_sales_funnel_targets_matrix_source.user_segment is not null
        and sheetload_sales_funnel_targets_matrix_source.user_geo is not null
        and sheetload_sales_funnel_targets_matrix_source.user_region is not null
        and sheetload_sales_funnel_targets_matrix_source.user_area is not null

),
user_hierarchy_stamped_opportunity as (
    /*
  To get a complete picture of the hierarchy and to ensure fidelity with the stamped opportunities, we will union in the distinct hierarchy values from the stamped opportunities.
*/
    select distinct
        dim_date.fiscal_year,
        sfdc_opportunity_source.user_segment_stamped as user_segment,
        sfdc_opportunity_source.user_geo_stamped as user_geo,
        sfdc_opportunity_source.user_region_stamped as user_region,
        sfdc_opportunity_source.user_area_stamped as user_area,
        sfdc_opportunity_source.user_segment_geo_region_area_stamped
        as user_segment_geo_region_area,
        coalesce(
            final_scd.is_last_user_hierarchy_in_fiscal_year, 1
        ) as is_last_user_hierarchy_in_fiscal_year,
        coalesce(
            final_scd.is_last_user_area_in_fiscal_year, 0
        ) as is_last_user_area_in_fiscal_year
    from sfdc_opportunity_source
    inner join dim_date on sfdc_opportunity_source.close_date = dim_date.date_actual
    left join
        final_scd on lower(
            sfdc_opportunity_source.user_segment_geo_region_area_stamped
        ) = lower(
            final_scd.user_segment_geo_region_area
        ) and dim_date.fiscal_year = final_scd.fiscal_year

),
unioned as (
    /*
  Full outer join with all three hierarchy sources and coalesce the fields, prioritizing the SFDC versions to maintain consistency in how the hierarchy appears
  The full outer join will allow all possible hierarchies to flow in from all three sources
*/
    select distinct
        coalesce(
            final_scd.user_segment,
            user_hierarchy_stamped_opportunity.user_segment,
            user_hierarchy_sheetload.user_segment
        ) as user_segment,
        coalesce(
            final_scd.user_geo,
            user_hierarchy_stamped_opportunity.user_geo,
            user_hierarchy_sheetload.user_geo
        ) as user_geo,
        coalesce(
            final_scd.user_region,
            user_hierarchy_stamped_opportunity.user_region,
            user_hierarchy_sheetload.user_region
        ) as user_region,
        coalesce(
            final_scd.user_area,
            user_hierarchy_stamped_opportunity.user_area,
            user_hierarchy_sheetload.user_area
        ) as user_area,
        coalesce(
            final_scd.user_segment_geo_region_area,
            user_hierarchy_stamped_opportunity.user_segment_geo_region_area,
            user_hierarchy_sheetload.user_segment_geo_region_area
        ) as user_segment_geo_region_area,
        coalesce(
            final_scd.fiscal_year,
            user_hierarchy_stamped_opportunity.fiscal_year,
            user_hierarchy_sheetload.fiscal_year
        ) as fiscal_year,
        coalesce(
            final_scd.is_last_user_hierarchy_in_fiscal_year,
            user_hierarchy_stamped_opportunity.is_last_user_hierarchy_in_fiscal_year,
            user_hierarchy_sheetload.is_last_user_hierarchy_in_fiscal_year
        ) as is_last_user_hierarchy_in_fiscal_year,
        coalesce(
            final_scd.is_last_user_area_in_fiscal_year,
            user_hierarchy_stamped_opportunity.is_last_user_area_in_fiscal_year,
            user_hierarchy_sheetload.is_last_user_area_in_fiscal_year
        ) as is_last_user_area_in_fiscal_year
    from final_scd
    full outer join
        user_hierarchy_stamped_opportunity on lower(
            user_hierarchy_stamped_opportunity.user_segment_geo_region_area
        ) = lower(
            final_scd.user_segment_geo_region_area
        ) and user_hierarchy_stamped_opportunity.fiscal_year = final_scd.fiscal_year
    full outer join
        user_hierarchy_sheetload on lower(
            user_hierarchy_sheetload.user_segment_geo_region_area
        ) = lower(
            final_scd.user_segment_geo_region_area
        ) and user_hierarchy_sheetload.fiscal_year = final_scd.fiscal_year

),
final as (

    select
        {{ dbt_utils.surrogate_key(["user_segment_geo_region_area", "fiscal_year"]) }}
        as dim_crm_user_hierarchy_stamped_id,
        {{ dbt_utils.surrogate_key(["user_segment"]) }}
        as dim_crm_opp_owner_sales_segment_stamped_id,
        user_segment as crm_opp_owner_sales_segment_stamped,
        {{ dbt_utils.surrogate_key(["user_geo"]) }} as dim_crm_opp_owner_geo_stamped_id,
        user_geo as crm_opp_owner_geo_stamped,
        {{ dbt_utils.surrogate_key(["user_region"]) }}
        as dim_crm_opp_owner_region_stamped_id,
        user_region as crm_opp_owner_region_stamped,
        {{ dbt_utils.surrogate_key(["user_area"]) }}
        as dim_crm_opp_owner_area_stamped_id,
        user_area as crm_opp_owner_area_stamped,
        user_segment_geo_region_area
        as crm_opp_owner_sales_segment_geo_region_area_stamped,
        case
            when user_segment in ('Large', 'PubSec') then 'Large' else user_segment
        end as crm_opp_owner_sales_segment_stamped_grouped,
        {{ sales_segment_region_grouped("user_segment", "user_geo", "user_region") }}
        as crm_opp_owner_sales_segment_region_stamped_grouped,
        fiscal_year,
        is_last_user_hierarchy_in_fiscal_year,
        is_last_user_area_in_fiscal_year
    from unioned

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@mcooperDD",
        updated_by="@jpeguero",
        created_date="2021-01-05",
        updated_date="2022-03-18",
    )
}}
