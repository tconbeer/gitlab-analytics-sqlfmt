{{ config(alias="report_pipeline_velocity_quarter") }}
with
    sfdc_opportunity_snapshot_history_xf as (

        select *
        from {{ ref("wk_sales_sfdc_opportunity_snapshot_history_xf") }}
        where is_deleted = 0 and is_edu_oss = 0 and is_excluded_flag = 0

    ),
    -- keys used for aggregated historical analysis
    agg_demo_keys as (select * from {{ ref("wk_sales_report_agg_demo_sqs_ot_keys") }}),
    report_pipeline_velocity_quarter as (

        select
            snapshot_date,
            snapshot_fiscal_quarter_name,
            snapshot_fiscal_quarter_date,
            snapshot_fiscal_year,
            snapshot_day_of_fiscal_quarter_normalised,
            close_day_of_fiscal_quarter_normalised,
            close_fiscal_quarter_name,
            close_fiscal_quarter_date,
            close_fiscal_year,

            -- -----------------------
            -- keys
            report_user_segment_geo_region_area_sqs_ot,
            -- -----------------------
            -- -----------------
            -- NF 2022-02-20 I have the feeling all these fields could be removed from
            -- the model
            -- They were originally added to support Fred's reporting needs but I
            -- don't think they are leveraged now
            /*
      stage_name_3plus,
      stage_name_4plus,
      is_stage_1_plus,
      is_stage_3_plus,
      is_stage_4_plus,
      is_open,
      is_lost,
      is_won,
      is_renewal,
      is_excluded_flag,
      stage_name,
      forecast_category_name,
      */
            sum(open_1plus_net_arr) as open_1plus_net_arr,
            sum(open_3plus_net_arr) as open_3plus_net_arr,
            sum(open_4plus_net_arr) as open_4plus_net_arr,
            sum(booked_net_arr) as booked_net_arr,
            sum(churned_contraction_net_arr) as churned_contraction_net_arr,
            sum(net_arr) as net_arr,

            sum(calculated_deal_count) as deal_count

        from sfdc_opportunity_snapshot_history_xf
        where
            -- 2 quarters before start and full quarter, total rolling 9 months at end
            -- of quarter
            -- till end of quarter
            snapshot_date <= dateadd(
                month, 3, close_fiscal_quarter_date
            -- 2 quarters before start
            ) and snapshot_date >= dateadd(month, -6, close_fiscal_quarter_date)
        -- , 11,12, 13, 14, 15, 16,17,18,19,20,21,22
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10

    ),
    final as (

        select
            agg.*,

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

            agg_demo_keys.report_user_segment_geo_region_area

        from report_pipeline_velocity_quarter agg
        left join
            agg_demo_keys
            on agg.report_user_segment_geo_region_area_sqs_ot
            = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot

    )

select *
from final
