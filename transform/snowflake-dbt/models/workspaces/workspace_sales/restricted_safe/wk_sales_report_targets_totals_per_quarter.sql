{{ config(alias="report_targets_totals_per_quarter") }}

with
    date_details as (select * from {{ ref("wk_sales_date_details") }}),
    agg_demo_keys as (
        -- keys used for aggregated historical analysis
        select * from {{ ref("wk_sales_report_agg_demo_sqs_ot_keys") }}

    ),
    sfdc_opportunity_snapshot_history_xf as (

        select *
        from {{ ref("wk_sales_sfdc_opportunity_snapshot_history_xf") }}
        where is_deleted = 0 and is_edu_oss = 0

    ),
    mart_sales_funnel_target as (

        select * from {{ ref("wk_sales_mart_sales_funnel_target") }}

    ),
    today_date as (

        select distinct
            first_day_of_fiscal_quarter as current_fiscal_quarter_date,
            fiscal_quarter_name_fy as current_fiscal_quarter_name,
            day_of_fiscal_quarter_normalised as current_day_of_fiscal_quarter_normalised
        from date_details
        where date_actual = current_date

    ),
    funnel_targets_per_quarter as (

        select
            target_fiscal_quarter_name,
            target_fiscal_quarter_date,
            -- -----------------------
            -- keys
            report_user_segment_geo_region_area_sqs_ot,
            -- -----------------------  
            sum(
                case when kpi_name = 'Net ARR' then allocated_target else 0 end
            ) as target_net_arr,
            sum(
                case when kpi_name = 'Deals' then allocated_target else 0 end
            ) as target_deal_count,
            sum(
                case
                    when kpi_name = 'Net ARR Pipeline Created'
                    then allocated_target
                    else 0
                end
            ) as target_pipe_generation_net_arr
        from mart_sales_funnel_target
        group by 1, 2, 3

    ),
    totals_per_quarter as (

        select
            opp_snapshot.snapshot_fiscal_quarter_name as close_fiscal_quarter_name,
            opp_snapshot.snapshot_fiscal_quarter_date as close_fiscal_quarter_date,
            -- -----------------------
            -- keys
            opp_snapshot.report_user_segment_geo_region_area_sqs_ot,
            -- -----------------------
            sum(
                case
                    when
                        opp_snapshot.close_fiscal_quarter_date
                        = opp_snapshot.snapshot_fiscal_quarter_date
                    then opp_snapshot.booked_net_arr
                    else 0
                end
            ) as total_booked_net_arr,
            sum(
                case
                    when
                        opp_snapshot.close_fiscal_quarter_date
                        = opp_snapshot.snapshot_fiscal_quarter_date
                    then opp_snapshot.churned_contraction_net_arr
                    else 0
                end
            ) as total_churned_contraction_net_arr,
            sum(
                case
                    when
                        opp_snapshot.close_fiscal_quarter_date
                        = opp_snapshot.snapshot_fiscal_quarter_date
                    then opp_snapshot.booked_deal_count
                    else 0
                end
            ) as total_booked_deal_count,
            sum(
                case
                    when
                        opp_snapshot.close_fiscal_quarter_date
                        = opp_snapshot.snapshot_fiscal_quarter_date
                    then opp_snapshot.churned_contraction_deal_count
                    else 0
                end
            ) as total_churned_contraction_deal_count,

            -- Pipe gen totals
            sum(
                case
                    when
                        opp_snapshot.pipeline_created_fiscal_quarter_date
                        = opp_snapshot.snapshot_fiscal_quarter_date
                        and opp_snapshot.is_eligible_created_pipeline_flag = 1
                    then opp_snapshot.created_in_snapshot_quarter_net_arr
                    else 0
                end
            ) as total_pipe_generation_net_arr,
            sum(
                case
                    when
                        opp_snapshot.pipeline_created_fiscal_quarter_date
                        = opp_snapshot.snapshot_fiscal_quarter_date
                        and opp_snapshot.is_eligible_created_pipeline_flag = 1
                    then opp_snapshot.created_in_snapshot_quarter_deal_count
                    else 0
                end
            ) as total_pipe_generation_deal_count,

            -- SAO totals per quarter
            sum(
                case
                    when
                        opp_snapshot.sales_accepted_fiscal_quarter_date
                        = opp_snapshot.snapshot_fiscal_quarter_date
                        and opp_snapshot.is_eligible_sao_flag = 1
                    then opp_snapshot.net_arr
                    else 0
                end
            ) as total_sao_generation_net_arr,
            sum(
                case
                    when
                        opp_snapshot.sales_accepted_fiscal_quarter_date
                        = opp_snapshot.snapshot_fiscal_quarter_date
                        and opp_snapshot.is_eligible_sao_flag = 1
                    then opp_snapshot.calculated_deal_count
                    else 0
                end
            ) as total_sao_generation_deal_count,

            -- Created & Landed totals
            sum(
                case
                    when
                        opp_snapshot.close_fiscal_quarter_date
                        = opp_snapshot.snapshot_fiscal_quarter_date
                    then opp_snapshot.created_and_won_same_quarter_net_arr
                    else 0
                end
            ) as total_created_and_booked_same_quarter_net_arr
        from sfdc_opportunity_snapshot_history_xf opp_snapshot
        where
            opp_snapshot.is_excluded_flag = 0
            and opp_snapshot.is_deleted = 0
            and opp_snapshot.snapshot_day_of_fiscal_quarter_normalised = 90
        group by 1, 2, 3

    ),
    base_fields as (

        select
            target_fiscal_quarter_name as close_fiscal_quarter_name,
            target_fiscal_quarter_date as close_fiscal_quarter_date,
            report_user_segment_geo_region_area_sqs_ot
        from funnel_targets_per_quarter
        union
        select
            close_fiscal_quarter_name,
            close_fiscal_quarter_date,
            report_user_segment_geo_region_area_sqs_ot
        from totals_per_quarter

    ),
    consolidated_targets_totals as (

        select
            -- ------
            -- Keys
            base.close_fiscal_quarter_name,
            base.close_fiscal_quarter_date,
            base.report_user_segment_geo_region_area_sqs_ot,
            -- ---
            report_date.fiscal_year as close_fiscal_year,

            -- ------------------------------------------------    
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

            agg_demo_keys.report_user_segment_geo_region_area,

            -- ------------------------------------------------
            coalesce(target.target_net_arr, 0) as target_net_arr,
            coalesce(target.target_deal_count, 0) as target_deal_count,
            coalesce(
                target.target_pipe_generation_net_arr, 0
            ) as target_pipe_generation_net_arr,

            coalesce(total.total_booked_net_arr, 0) as total_booked_net_arr,
            coalesce(
                total.total_churned_contraction_net_arr, 0
            ) as total_churned_contraction_net_arr,
            coalesce(total.total_booked_deal_count, 0) as total_booked_deal_count,
            coalesce(
                total.total_churned_contraction_deal_count, 0
            ) as total_churned_contraction_deal_count,
            coalesce(
                total.total_pipe_generation_net_arr, 0
            ) as total_pipe_generation_net_arr,
            coalesce(
                total.total_pipe_generation_deal_count, 0
            ) as total_pipe_generation_deal_count,

            coalesce(
                total.total_sao_generation_net_arr, 0
            ) as total_sao_generation_net_arr,
            coalesce(
                total.total_sao_generation_deal_count, 0
            ) as total_sao_generation_deal_count,

            coalesce(
                total.total_created_and_booked_same_quarter_net_arr, 0
            ) as total_created_and_booked_same_quarter_net_arr,

            -- check if we are in the current quarter or not. If not, use total, if we
            -- are use taret
            case
                when
                    today_date.current_fiscal_quarter_date
                    <= base.close_fiscal_quarter_date
                then target.target_net_arr
                else total.total_booked_net_arr
            end as calculated_target_net_arr,
            case
                when
                    today_date.current_fiscal_quarter_date
                    <= base.close_fiscal_quarter_date
                then target.target_deal_count
                else total.total_booked_deal_count
            end as calculated_target_deal_count,
            case
                when
                    today_date.current_fiscal_quarter_date
                    <= base.close_fiscal_quarter_date
                then target.target_pipe_generation_net_arr
                else total.total_pipe_generation_net_arr
            end as calculated_target_pipe_generation
        from base_fields base
        cross join today_date
        inner join
            date_details report_date
            on report_date.date_actual = base.close_fiscal_quarter_date
        left join
            agg_demo_keys
            on base.report_user_segment_geo_region_area_sqs_ot
            = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
        left join
            funnel_targets_per_quarter target
            on target.target_fiscal_quarter_date = base.close_fiscal_quarter_date
            and target.report_user_segment_geo_region_area_sqs_ot
            = base.report_user_segment_geo_region_area_sqs_ot
        -- quarterly total
        left join
            totals_per_quarter total
            on total.close_fiscal_quarter_date = base.close_fiscal_quarter_date
            and total.report_user_segment_geo_region_area_sqs_ot
            = base.report_user_segment_geo_region_area_sqs_ot

    )
select *
from consolidated_targets_totals
