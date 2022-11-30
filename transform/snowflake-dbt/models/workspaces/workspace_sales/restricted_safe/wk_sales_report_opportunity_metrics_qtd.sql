{{ config(alias="report_opportunity_metrics_qtd") }}

with
    sfdc_opportunity_xf as (

        select *
        from {{ ref("wk_sales_sfdc_opportunity_xf") }}
        where is_edu_oss = 0 and is_deleted = 0

    ),
    sfdc_opportunity_snapshot_history_xf as (


        select *
        from {{ ref("wk_sales_sfdc_opportunity_snapshot_history_xf") }}
        where is_edu_oss = 0 and is_deleted = 0

    ),
    date_details as (select * from {{ ref("wk_sales_date_details") }}),
    -- keys used for aggregated historical analysis
    agg_demo_keys as (select * from {{ ref("wk_sales_report_agg_demo_sqs_ot_keys") }}),
    today as (

        select distinct
            date_actual as current_date_actual,
            fiscal_quarter_name_fy as current_fiscal_quarter_name,
            first_day_of_fiscal_quarter as current_fiscal_quarter_date,
            day_of_fiscal_quarter_normalised as current_fiscal_quarter_day_normalised
        from date_details
        where date_actual = current_date

    ),
    aggregation as (

        select
            -- ------------------------------------------------------------------------------
            -- ------------------------------------------------------------------------------
            -- KEYS
            oppty.report_user_segment_geo_region_area_sqs_ot,
            today.current_fiscal_quarter_name,
            today.current_fiscal_quarter_day_normalised,

            -- ------------------------------------------------------------------------------
            -- ------------------------------------------------------------------------------
            sum(
                case
                    when
                        oppty.close_fiscal_quarter_name
                        = today.current_fiscal_quarter_name
                        and oppty.is_stage_1_plus = 1
                        and oppty.is_eligible_open_pipeline_flag = 1
                    then oppty.calculated_deal_count
                    else 0
                end
            ) as qtd_open_1plus_deal_count,

            sum(
                case
                    when
                        oppty.close_fiscal_quarter_name
                        = today.current_fiscal_quarter_name
                        and oppty.is_stage_3_plus = 1
                        and oppty.is_eligible_open_pipeline_flag = 1
                    then oppty.calculated_deal_count
                    else 0
                end
            ) as qtd_open_3plus_deal_count,

            sum(
                case
                    when
                        oppty.close_fiscal_quarter_name
                        = today.current_fiscal_quarter_name
                        and oppty.is_stage_4_plus = 1
                        and oppty.is_eligible_open_pipeline_flag = 1
                    then oppty.calculated_deal_count
                    else 0
                end
            ) as qtd_open_4plus_deal_count,

            sum(
                case
                    when
                        oppty.close_fiscal_quarter_name
                        = today.current_fiscal_quarter_name
                        and oppty.is_won = 1
                    then oppty.calculated_deal_count
                    else 0
                end
            ) as qtd_closed_deal_count,

            -- CREATED in quarter
            -- NF: 2020-02-18 Need to validate how do I consider lost deals, a lost
            -- deal that is coming from a non- 1+stage should not oppty. included here
            sum(
                case
                    when
                        oppty.pipeline_created_fiscal_quarter_name
                        = today.current_fiscal_quarter_name
                        and oppty.is_eligible_created_pipeline_flag = 1
                    then oppty.calculated_deal_count
                    else 0
                end
            ) as qtd_created_deal_count,

            -- NEXT Q Deal count
            -- Net ARR
            sum(
                case
                    when
                        oppty.close_fiscal_quarter_name
                        = today.current_fiscal_quarter_name
                    then oppty.booked_net_arr
                    else 0
                end
            ) as qtd_booked_net_arr,

            sum(
                case
                    when
                        oppty.close_fiscal_quarter_name
                        = today.current_fiscal_quarter_name
                        and oppty.is_eligible_open_pipeline_flag = 1
                        and oppty.is_stage_1_plus = 1
                    then oppty.net_arr
                    else 0
                end
            ) as qtd_open_1plus_net_arr,

            sum(
                case
                    when
                        oppty.close_fiscal_quarter_name
                        = today.current_fiscal_quarter_name
                        and oppty.is_eligible_open_pipeline_flag = 1
                        and oppty.is_stage_3_plus = 1
                    then oppty.net_arr
                    else 0
                end
            ) as qtd_open_3plus_net_arr,

            sum(
                case
                    when
                        oppty.close_fiscal_quarter_name
                        = today.current_fiscal_quarter_name
                        and oppty.is_eligible_open_pipeline_flag = 1
                        and oppty.is_stage_4_plus = 1
                    then oppty.net_arr
                    else 0
                end
            ) as qtd_open_4plus_net_arr,

            -- created pipeline in quarter
            sum(
                case
                    when
                        oppty.pipeline_created_fiscal_quarter_name
                        = today.current_fiscal_quarter_name
                        and oppty.is_eligible_created_pipeline_flag = 1
                    then oppty.net_arr
                    else 0
                end
            ) as qtd_created_net_arr,

            -- created and closed in the same quarter
            sum(
                case
                    when
                        oppty.close_fiscal_quarter_name = current_fiscal_quarter_name
                        and oppty.pipeline_created_fiscal_quarter_name
                        = today.current_fiscal_quarter_name
                        and oppty.is_eligible_created_pipeline_flag = 1
                        and (
                            oppty.is_won = 1
                            or (oppty.is_lost = 1 and oppty.is_renewal = 1)
                        )
                    then oppty.net_arr
                    else 0
                end
            ) as qtd_created_and_closed_net_arr,

            -- ---------------------------------------------------------------------------------------------------------
            -- ---------------------------------------------------------------------------------------------------------
            sum(
                case
                    when
                        oppty.close_fiscal_quarter_date
                        = dateadd(month, 3, today.current_fiscal_quarter_date)
                        and oppty.is_stage_1_plus = 1
                        and oppty.is_eligible_open_pipeline_flag = 1
                    then oppty.calculated_deal_count
                    else 0
                end
            ) as rq_plus_1_open_1plus_deal_count,

            sum(
                case
                    when
                        oppty.close_fiscal_quarter_date
                        = dateadd(month, 3, today.current_fiscal_quarter_date)
                        and oppty.is_stage_3_plus = 1
                        and oppty.is_eligible_open_pipeline_flag = 1
                    then oppty.calculated_deal_count
                    else 0
                end
            ) as rq_plus_1_open_3plus_deal_count,

            sum(
                case
                    when
                        oppty.close_fiscal_quarter_date
                        = dateadd(month, 3, today.current_fiscal_quarter_date)
                        and oppty.is_stage_4_plus = 1
                        and oppty.is_eligible_open_pipeline_flag = 1
                    then oppty.calculated_deal_count
                    else 0
                end
            ) as rq_plus_1_open_4plus_deal_count,

            -- next quarter net arr
            sum(
                case
                    when
                        oppty.close_fiscal_quarter_date
                        = dateadd(month, 3, today.current_fiscal_quarter_date)
                        and oppty.is_eligible_open_pipeline_flag = 1
                        and oppty.is_stage_1_plus = 1
                    then oppty.net_arr
                    else 0
                end
            ) as rq_plus_1_open_1plus_net_arr,

            -- next quarter 3+
            sum(
                case
                    when
                        oppty.close_fiscal_quarter_date
                        = dateadd(month, 3, today.current_fiscal_quarter_date)
                        and oppty.is_eligible_open_pipeline_flag = 1
                        and oppty.is_stage_3_plus = 1
                    then oppty.net_arr
                    else 0
                end
            ) as rq_plus_1_open_3plus_net_arr,

            -- next quarter 4+
            sum(
                case
                    when
                        oppty.close_fiscal_quarter_date
                        = dateadd(month, 3, today.current_fiscal_quarter_date)
                        and oppty.is_eligible_open_pipeline_flag = 1
                        and oppty.is_stage_4_plus = 1
                    then oppty.net_arr
                    else 0
                end
            ) as rq_plus_1_open_4plus_net_arr,

            -- ---------------------------------------------------------------------------------------------------------
            -- ---------------------------------------------------------------------------------------------------------
            -- quarter + 2
            -- DEAL COUNT
            sum(
                case
                    when
                        oppty.close_fiscal_quarter_date
                        = dateadd(month, 6, today.current_fiscal_quarter_date)
                        and oppty.is_eligible_open_pipeline_flag = 1
                        and oppty.is_stage_1_plus = 1
                    then oppty.calculated_deal_count
                    else 0
                end
            ) as rq_plus_2_open_1plus_deal_count,

            sum(
                case
                    when
                        oppty.close_fiscal_quarter_date
                        = dateadd(month, 6, today.current_fiscal_quarter_date)
                        and oppty.is_eligible_open_pipeline_flag = 1
                        and oppty.is_stage_3_plus = 1
                    then oppty.calculated_deal_count
                    else 0
                end
            ) as rq_plus_2_open_3plus_deal_count,

            sum(
                case
                    when
                        oppty.close_fiscal_quarter_date
                        = dateadd(month, 6, today.current_fiscal_quarter_date)
                        and oppty.is_eligible_open_pipeline_flag = 1
                        and oppty.is_stage_4_plus = 1
                    then oppty.calculated_deal_count
                    else 0
                end
            ) as rq_plus_2_open_4plus_deal_count,
            -- -----------
            -- NET ARR
            sum(
                case
                    when
                        oppty.close_fiscal_quarter_date
                        = dateadd(month, 6, today.current_fiscal_quarter_date)
                        and oppty.is_eligible_open_pipeline_flag = 1
                        and oppty.is_stage_1_plus = 1
                    then oppty.net_arr
                    else 0
                end
            ) as rq_plus_2_open_1plus_net_arr,

            sum(
                case
                    when
                        oppty.close_fiscal_quarter_date
                        = dateadd(month, 6, today.current_fiscal_quarter_date)
                        and oppty.is_eligible_open_pipeline_flag = 1
                        and oppty.is_stage_3_plus = 1
                    then oppty.net_arr
                    else 0
                end
            ) as rq_plus_2_open_3plus_net_arr,

            sum(
                case
                    when
                        oppty.close_fiscal_quarter_date
                        = dateadd(month, 6, today.current_fiscal_quarter_date)
                        and oppty.is_eligible_open_pipeline_flag = 1
                        and oppty.is_stage_4_plus = 1
                    then oppty.net_arr
                    else 0
                end
            ) as rq_plus_2_open_4plus_net_arr



        from sfdc_opportunity_xf oppty
        -- identify todays quarter and fiscal quarter
        cross join today
        group by 1, 2, 3

    ),
    pipe_gen_yoy as (

        select
            opp_snapshot.report_user_segment_geo_region_area_sqs_ot,
            sum(opp_snapshot.net_arr) as minus_1_year_pipe_gen_net_arr
        from sfdc_opportunity_snapshot_history_xf opp_snapshot
        cross join today
        where
            opp_snapshot.snapshot_fiscal_quarter_date
            = opp_snapshot.pipeline_created_fiscal_quarter_date
            and opp_snapshot.is_eligible_created_pipeline_flag = 1
            and opp_snapshot.snapshot_fiscal_quarter_date
            = dateadd(month, -12, today.current_fiscal_quarter_date)
            and opp_snapshot.snapshot_day_of_fiscal_quarter_normalised
            = today.current_fiscal_quarter_day_normalised
            and opp_snapshot.is_edu_oss = 0
            and opp_snapshot.is_deleted = 0
            and opp_snapshot.is_excluded_flag = 0
            and lower(opp_snapshot.deal_group) like any ('%growth%', '%new%')
        group by 1

    ),
    report_opportunity_metrics_qtd as (

        select
            agg.*,
            pipe_gen_yoy.minus_1_year_pipe_gen_net_arr,

            -- standard reporting keys
            coalesce(agg_demo_keys.key_sqs, 'other') as key_sqs,
            coalesce(agg_demo_keys.key_ot, 'other') as key_ot,

            coalesce(agg_demo_keys.key_segment, 'other') as key_segment,
            coalesce(agg_demo_keys.key_segment_sqs, 'other') as key_segment_sqs,
            coalesce(agg_demo_keys.key_segment_ot, 'other') as key_segment_ot,

            coalesce(agg_demo_keys.key_segment_geo, 'other') as key_segment_geo,
            coalesce(agg_demo_keys.key_segment_geo_sqs, 'other') as key_segment_geo_sqs,
            coalesce(agg_demo_keys.key_segment_geo_ot, 'other') as key_segment_geo_ot,

            coalesce(
                agg_demo_keys.key_segment_geo_region, 'other'
            ) as key_segment_geo_region,
            coalesce(
                agg_demo_keys.key_segment_geo_region_sqs, 'other'
            ) as key_segment_geo_region_sqs,
            coalesce(
                agg_demo_keys.key_segment_geo_region_ot, 'other'
            ) as key_segment_geo_region_ot,

            coalesce(
                agg_demo_keys.key_segment_geo_region_area, 'other'
            ) as key_segment_geo_region_area,
            coalesce(
                agg_demo_keys.key_segment_geo_region_area_sqs, 'other'
            ) as key_segment_geo_region_area_sqs,
            coalesce(
                agg_demo_keys.key_segment_geo_region_area_ot, 'other'
            ) as key_segment_geo_region_area_ot,

            coalesce(
                agg_demo_keys.report_opportunity_user_segment, 'other'
            ) as sales_team_cro_level,

            -- NF: This code replicates the reporting structured of FY22, to keep
            -- current tools working
            coalesce(
                agg_demo_keys.sales_team_rd_asm_level, 'other'
            ) as sales_team_rd_asm_level,
            coalesce(agg_demo_keys.sales_team_vp_level, 'other') as sales_team_vp_level,
            coalesce(
                agg_demo_keys.sales_team_avp_rd_level, 'other'
            ) as sales_team_avp_rd_level,
            coalesce(
                agg_demo_keys.sales_team_asm_level, 'other'
            ) as sales_team_asm_level

        from aggregation agg
        -- Add keys for aggregated analysis
        left join
            pipe_gen_yoy
            on agg.report_user_segment_geo_region_area_sqs_ot
            = pipe_gen_yoy.report_user_segment_geo_region_area_sqs_ot
        left join
            agg_demo_keys
            on agg.report_user_segment_geo_region_area_sqs_ot
            = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
    )

select *
from report_opportunity_metrics_qtd
