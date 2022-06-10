{{ config(alias="report_pipeline_velocity_quarter_with_targets") }}

-- TODO:
-- NF: Refactor open X metrics to use new fields created in opportunity / snapshot
-- objects
with
    report_pipeline_velocity_quarter as (

        select * from {{ ref("wk_sales_report_pipeline_velocity_quarter") }}

    ),
    date_details as (select * from {{ ref("wk_sales_date_details") }}),
    today_date as (

        select distinct
            first_day_of_fiscal_quarter as current_fiscal_quarter_date,
            fiscal_quarter_name_fy as current_fiscal_quarter_name,
            day_of_fiscal_quarter_normalised as current_day_of_fiscal_quarter_normalised
        from date_details
        where date_actual = current_date

    ),
    -- keys used for aggregated historical analysis
    agg_demo_keys as (select * from {{ ref("wk_sales_report_agg_demo_sqs_ot_keys") }}),
    sfdc_opportunity_xf as (

        select *
        from {{ ref("wk_sales_sfdc_opportunity_xf") }}
        cross join today_date
        where is_excluded_flag = 0 and is_edu_oss = 0 and is_deleted = 0

    ),
    report_targets_totals_per_quarter as (

        select * from {{ ref("wk_sales_report_targets_totals_per_quarter") }}

    ),
    report_pipeline_velocity as (

        select * from report_pipeline_velocity_quarter cross join today_date

    ),
    consolidated_targets_totals as (

        select
            base.close_fiscal_quarter_name,
            base.close_fiscal_quarter_date,
            -- -----------------------
            -- keys
            base.report_user_segment_geo_region_area_sqs_ot,
            -- -----------------------
            base.target_net_arr,
            base.total_churned_contraction_net_arr,
            base.total_churned_contraction_deal_count,
            base.total_booked_net_arr as total_net_arr,
            base.calculated_target_net_arr as adjusted_target_net_arr
        from report_targets_totals_per_quarter base

    ),
    pipeline_summary as (

        select
            pv.close_fiscal_quarter_name,
            pv.close_fiscal_quarter_date,
            pv.close_day_of_fiscal_quarter_normalised,

            -- -----------------------
            -- keys
            pv.report_user_segment_geo_region_area_sqs_ot,
            -- -----------------------
            sum(pv.open_1plus_net_arr) as open_1plus_net_arr,
            sum(pv.open_3plus_net_arr) as open_3plus_net_arr,
            sum(pv.open_4plus_net_arr) as open_4plus_net_arr,
            sum(pv.booked_net_arr) as booked_net_arr,
            sum(pv.churned_contraction_net_arr) as churned_contraction_net_arr

        from report_pipeline_velocity pv
        where
            pv.close_fiscal_year >= 2020 and (
                pv.close_day_of_fiscal_quarter_normalised
                != pv.current_day_of_fiscal_quarter_normalised
                or pv.close_fiscal_quarter_date != pv.current_fiscal_quarter_date
            )
        group by 1, 2, 3, 4
        UNION
        -- to have the same current values as in X-Ray
        select
            o.close_fiscal_quarter_name,
            o.close_fiscal_quarter_date,
            o.current_day_of_fiscal_quarter_normalised,

            -- -----------------------
            -- keys
            o.report_user_segment_geo_region_area_sqs_ot,
            -- -----------------------     
            sum(o.open_1plus_net_arr) as open_1plus_net_arr,
            sum(o.open_3plus_net_arr) as open_3plus_net_arr,
            sum(o.open_4plus_net_arr) as open_4plus_net_arr,
            sum(o.booked_net_arr) as booked_net_arr,
            sum(o.churned_contraction_net_arr) as churned_contraction_net_arr

        from sfdc_opportunity_xf o
        where o.close_fiscal_quarter_name = o.current_fiscal_quarter_name
        group by 1, 2, 3, 4

    ),
    base_keys as (


        select
            pipeline_summary.close_fiscal_quarter_name,
            pipeline_summary.close_fiscal_quarter_date,
            pipeline_summary.close_day_of_fiscal_quarter_normalised,
            pipeline_summary.report_user_segment_geo_region_area_sqs_ot
        from pipeline_summary
        UNION
        select
            consolidated_targets_totals.close_fiscal_quarter_name,
            consolidated_targets_totals.close_fiscal_quarter_date,
            close_day.close_day_of_fiscal_quarter_normalised,
            consolidated_targets_totals.report_user_segment_geo_region_area_sqs_ot
        from consolidated_targets_totals
        cross join
            (
                select distinct close_day_of_fiscal_quarter_normalised
                from pipeline_summary
            ) close_day


    ),
    pipeline_velocity_with_targets_per_day as (

        select distinct

            base.close_fiscal_quarter_name,
            base.close_fiscal_quarter_date,
            base.close_day_of_fiscal_quarter_normalised,

            -- -----------------------
            -- keys
            base.report_user_segment_geo_region_area_sqs_ot,
            -- -----------------------
            target.total_churned_contraction_net_arr,
            target.total_churned_contraction_deal_count,

            target.total_net_arr,
            target.target_net_arr,
            target.adjusted_target_net_arr,

            ps.open_1plus_net_arr,
            ps.open_3plus_net_arr,
            ps.open_4plus_net_arr,
            ps.booked_net_arr,
            ps.churned_contraction_net_arr

        from base_keys base
        left join
            consolidated_targets_totals target
            on target.close_fiscal_quarter_name = base.close_fiscal_quarter_name
            and target.report_user_segment_geo_region_area_sqs_ot
            = base.report_user_segment_geo_region_area_sqs_ot
        left join
            pipeline_summary ps
            on base.close_fiscal_quarter_name = ps.close_fiscal_quarter_name
            and base.close_day_of_fiscal_quarter_normalised
            = ps.close_day_of_fiscal_quarter_normalised
            and base.report_user_segment_geo_region_area_sqs_ot
            = ps.report_user_segment_geo_region_area_sqs_ot
        -- only consider quarters we have data in the snapshot history
        where
            base.close_fiscal_quarter_date >= '2019-08-01'::date
            and base.close_day_of_fiscal_quarter_normalised <= 90

    ),
    final as (

        select
            agg.*,

            agg_demo_keys.report_opportunity_user_segment,
            agg_demo_keys.report_opportunity_user_geo,
            agg_demo_keys.report_opportunity_user_region,
            agg_demo_keys.report_opportunity_user_area,

            agg_demo_keys.sales_team_cro_level,
            agg_demo_keys.sales_team_vp_level,
            agg_demo_keys.sales_team_avp_rd_level,
            agg_demo_keys.sales_team_asm_level,
            agg_demo_keys.deal_category,
            agg_demo_keys.deal_group,
            agg_demo_keys.sales_qualified_source,
            agg_demo_keys.sales_team_rd_asm_level,

            agg_demo_keys.key_sqs,
            agg_demo_keys.key_ot,

            agg_demo_keys.key_segment,
            agg_demo_keys.key_segment_sqs,
            agg_demo_keys.key_segment_ot,

            agg_demo_keys.key_segment_geo,
            agg_demo_keys.key_segment_geo_sqs,
            agg_demo_keys.key_segment_geo_ot,

            agg_demo_keys.key_segment_geo_region,
            agg_demo_keys.key_segment_geo_region_sqs,
            agg_demo_keys.key_segment_geo_region_ot,

            agg_demo_keys.key_segment_geo_region_area,
            agg_demo_keys.key_segment_geo_region_area_sqs,
            agg_demo_keys.key_segment_geo_region_area_ot,

            agg_demo_keys.key_segment_geo_area,

            agg_demo_keys.report_user_segment_geo_region_area

        from pipeline_velocity_with_targets_per_day agg
        left join
            agg_demo_keys
            on agg.report_user_segment_geo_region_area_sqs_ot
            = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot


    )

select *
from final
