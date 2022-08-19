{{ config(alias="report_pipeline_metrics_day_with_targets") }}

with
    date_details as (select * from {{ ref("wk_sales_date_details") }}),
    report_pipeline_metrics_day as (

        select * from {{ ref("wk_sales_report_pipeline_metrics_per_day") }}

    ),
    report_targets_totals_per_quarter as (

        select * from {{ ref("wk_sales_report_targets_totals_per_quarter") }}

    ),
    mart_sales_funnel_target_daily as (

        select * from {{ ref("wk_sales_mart_sales_funnel_target_daily") }}

    ),
    -- keys used for aggregated historical analysis
    -- make sure the aggregation works at the level we want it
    agg_demo_keys as (select * from {{ ref("wk_sales_report_agg_demo_sqs_ot_keys") }}),
    consolidated_metrics as (

        select
            -- -------------------------
            -- Keys
            report_user_segment_geo_region_area_sqs_ot,
            -- ---------------------------
            close_fiscal_quarter_date,
            close_fiscal_quarter_name,
            close_day_of_fiscal_quarter_normalised,

            -- reported quarter
            sum(deal_count) as deal_count,
            sum(open_1plus_deal_count) as open_1plus_deal_count,
            sum(open_3plus_deal_count) as open_3plus_deal_count,
            sum(open_4plus_deal_count) as open_4plus_deal_count,
            sum(booked_deal_count) as booked_deal_count,
            sum(churned_contraction_deal_count) as churned_contraction_deal_count,



            -- reported quarter + 1
            sum(rq_plus_1_open_1plus_deal_count) as rq_plus_1_open_1plus_deal_count,
            sum(rq_plus_1_open_3plus_deal_count) as rq_plus_1_open_3plus_deal_count,
            sum(rq_plus_1_open_4plus_deal_count) as rq_plus_1_open_4plus_deal_count,

            -- reported quarter + 2
            sum(rq_plus_2_open_1plus_deal_count) as rq_plus_2_open_1plus_deal_count,
            sum(rq_plus_2_open_3plus_deal_count) as rq_plus_2_open_3plus_deal_count,
            sum(rq_plus_2_open_4plus_deal_count) as rq_plus_2_open_4plus_deal_count,

            -- ----------------------------
            -- Net ARR 
            -- Use Net ARR instead     
            -- created and closed
            -- reported quarter
            sum(booked_net_arr) as booked_net_arr,
            sum(churned_contraction_net_arr) as churned_contraction_net_arr,

            sum(open_1plus_net_arr) as open_1plus_net_arr,
            sum(open_3plus_net_arr) as open_3plus_net_arr,
            sum(open_4plus_net_arr) as open_4plus_net_arr,

            sum(
                created_and_won_same_quarter_net_arr
            ) as created_and_won_same_quarter_net_arr,

            -- reported quarter + 1
            sum(rq_plus_1_open_1plus_net_arr) as rq_plus_1_open_1plus_net_arr,
            sum(rq_plus_1_open_3plus_net_arr) as rq_plus_1_open_3plus_net_arr,
            sum(rq_plus_1_open_4plus_net_arr) as rq_plus_1_open_4plus_net_arr,

            -- reported quarter + 2
            sum(rq_plus_2_open_1plus_net_arr) as rq_plus_2_open_1plus_net_arr,
            sum(rq_plus_2_open_3plus_net_arr) as rq_plus_2_open_3plus_net_arr,
            sum(rq_plus_2_open_4plus_net_arr) as rq_plus_2_open_4plus_net_arr,

            -- pipe gen
            sum(pipe_gen_count) as pipe_gen_count,
            sum(pipe_gen_net_arr) as pipe_gen_net_arr,

            -- sao deal count
            sum(sao_deal_count) as sao_deal_count,
            sum(sao_net_arr) as sao_net_arr,

            -- one year ago pipe gen
            sum(minus_1_year_pipe_gen_net_arr) as minus_1_year_pipe_gen_net_arr,
            sum(minus_1_year_pipe_gen_deal_count) as minus_1_year_pipe_gen_deal_count,

            -- one year ago sao
            sum(minus_1_year_sao_net_arr) as minus_1_year_sao_net_arr,
            sum(minus_1_year_sao_deal_count) as minus_1_year_sao_deal_count


        from report_pipeline_metrics_day
        where close_day_of_fiscal_quarter_normalised > 0
        group by 1, 2, 3, 4

    ),
    consolidated_targets as (

        select
            -- -------------------------
            -- Keys
            report_user_segment_geo_region_area_sqs_ot,
            -- ---------------------------
            close_fiscal_quarter_name,
            close_fiscal_quarter_date,

            close_fiscal_year,

            sum(target_net_arr) as target_net_arr,
            sum(target_deal_count) as target_deal_count,
            sum(target_pipe_generation_net_arr) as target_pipe_generation_net_arr,

            sum(total_booked_net_arr) as total_booked_net_arr,
            sum(total_churned_contraction_net_arr) as total_churned_contraction_net_arr,
            sum(total_booked_deal_count) as total_booked_deal_count,
            sum(
                total_churned_contraction_deal_count
            ) as total_churned_contraction_deal_count,
            sum(total_pipe_generation_net_arr) as total_pipe_generation_net_arr,
            sum(total_pipe_generation_deal_count) as total_pipe_generation_deal_count,
            sum(
                total_created_and_booked_same_quarter_net_arr
            ) as total_created_and_booked_same_quarter_net_arr,
            sum(total_sao_generation_net_arr) as total_sao_generation_net_arr,
            sum(total_sao_generation_deal_count) as total_sao_generation_deal_count,

            sum(calculated_target_net_arr) as calculated_target_net_arr,
            sum(calculated_target_deal_count) as calculated_target_deal_count,
            sum(calculated_target_pipe_generation) as calculated_target_pipe_generation
        from report_targets_totals_per_quarter
        group by 1, 2, 3, 4

    ),
    consolidated_targets_per_day as (

        select targets.*, close_day_of_fiscal_quarter_normalised
        from consolidated_targets targets
        cross join
            (
                select
                    day_of_fiscal_quarter_normalised
                    as close_day_of_fiscal_quarter_normalised
                from date_details
                where day_of_fiscal_quarter_normalised > 0
                group by 1
            )


    -- some of the funnel metrics have daily targets with a very specific seasonality
    -- this models tracks the target allocated a given point in time on the quarter
    ),
    funnel_allocated_targets_qtd as (

        select
            target_fiscal_quarter_date as close_fiscal_quarter_date,
            target_day_of_fiscal_quarter_normalised
            as close_day_of_fiscal_quarter_normalised,

            -- ------------------------
            report_user_segment_geo_region_area_sqs_ot,

            -- ------------------------
            sum(
                case when kpi_name = 'Net ARR' then qtd_allocated_target else 0 end
            ) as qtd_target_net_arr,
            sum(
                case when kpi_name = 'Deals' then qtd_allocated_target else 0 end
            ) as qtd_target_deal_count,
            sum(
                case
                    when kpi_name = 'Net ARR Pipeline Created'
                    then qtd_allocated_target
                    else 0
                end
            ) as qtd_target_pipe_generation_net_arr
        from mart_sales_funnel_target_daily
        group by 1, 2, 3

    ),
    key_fields as (

        select report_user_segment_geo_region_area_sqs_ot, close_fiscal_quarter_date
        from consolidated_targets
        union
        select report_user_segment_geo_region_area_sqs_ot, close_fiscal_quarter_date
        from consolidated_metrics

    ),
    base_fields as (

        select
            key_fields.*,
            close_date.fiscal_quarter_name_fy as close_fiscal_quarter_name,
            close_date.fiscal_year as close_fiscal_year,
            close_date.day_of_fiscal_quarter_normalised
            as close_day_of_fiscal_quarter_normalised,
            close_date.date_actual as close_date,
            rq_plus_1.first_day_of_fiscal_quarter
            as rq_plus_1_close_fiscal_quarter_date,
            rq_plus_1.fiscal_quarter_name_fy as rq_plus_1_close_fiscal_quarter_name,
            rq_plus_2.first_day_of_fiscal_quarter
            as rq_plus_2_close_fiscal_quarter_date,
            rq_plus_2.fiscal_quarter_name_fy as rq_plus_2_close_fiscal_quarter_name
        from key_fields
        inner join
            date_details close_date
            on close_date.first_day_of_fiscal_quarter
            = key_fields.close_fiscal_quarter_date
        left join
            date_details rq_plus_1
            on rq_plus_1.date_actual
            = dateadd(month, 3, close_date.first_day_of_fiscal_quarter)
        left join
            date_details rq_plus_2
            on rq_plus_2.date_actual
            = dateadd(month, 6, close_date.first_day_of_fiscal_quarter)

    ),
    final as (

        select

            -- ------------------------
            -- keys
            base.report_user_segment_geo_region_area_sqs_ot,
            -- ------------------------
            base.close_fiscal_quarter_date,
            base.close_fiscal_quarter_name,
            base.close_fiscal_year,
            base.close_date,
            base.close_day_of_fiscal_quarter_normalised,

            -- --------------------------------------
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

            agg_demo_keys.report_user_segment_geo_region_area,
            -- --------------------------------------
            -- report quarter plus 1 / 2 date fields
            base.rq_plus_1_close_fiscal_quarter_name,
            base.rq_plus_1_close_fiscal_quarter_date,
            base.rq_plus_2_close_fiscal_quarter_name,
            base.rq_plus_2_close_fiscal_quarter_date,

            -- reported quarter
            metrics.deal_count,
            metrics.open_1plus_deal_count,
            metrics.open_3plus_deal_count,
            metrics.open_4plus_deal_count,
            metrics.booked_deal_count,
            metrics.churned_contraction_deal_count,

            -- reported quarter + 1
            metrics.rq_plus_1_open_1plus_deal_count,
            metrics.rq_plus_1_open_3plus_deal_count,
            metrics.rq_plus_1_open_4plus_deal_count,

            -- reported quarter + 2
            metrics.rq_plus_2_open_1plus_deal_count,
            metrics.rq_plus_2_open_3plus_deal_count,
            metrics.rq_plus_2_open_4plus_deal_count,

            -- ----------------------------
            -- Net ARR 
            -- Use Net ARR instead     
            -- created and closed
            -- reported quarter
            metrics.booked_net_arr,
            metrics.churned_contraction_net_arr,
            metrics.open_1plus_net_arr,
            metrics.open_3plus_net_arr,
            metrics.open_4plus_net_arr,

            -- pipe gen
            metrics.created_and_won_same_quarter_net_arr,
            metrics.pipe_gen_count,
            metrics.pipe_gen_net_arr,

            -- sao gen
            metrics.sao_deal_count,
            metrics.sao_net_arr,

            -- one year ago pipe gen
            metrics.minus_1_year_pipe_gen_net_arr,
            metrics.minus_1_year_pipe_gen_deal_count,

            -- one year ago sao
            metrics.minus_1_year_sao_net_arr,
            metrics.minus_1_year_sao_deal_count,

            -- reported quarter + 1
            metrics.rq_plus_1_open_1plus_net_arr,
            metrics.rq_plus_1_open_3plus_net_arr,
            metrics.rq_plus_1_open_4plus_net_arr,

            -- reported quarter + 2
            metrics.rq_plus_2_open_1plus_net_arr,
            metrics.rq_plus_2_open_3plus_net_arr,
            metrics.rq_plus_2_open_4plus_net_arr,

            -- targets current quarter
            coalesce(targets.target_net_arr, 0) as target_net_arr,
            coalesce(targets.target_deal_count, 0) as target_deal_count,
            coalesce(
                targets.target_pipe_generation_net_arr, 0
            ) as target_pipe_generation_net_arr,

            coalesce(targets.total_booked_net_arr, 0) as total_booked_net_arr,
            coalesce(
                targets.total_churned_contraction_net_arr, 0
            ) as total_churned_contraction_net_arr,
            coalesce(targets.total_booked_deal_count, 0) as total_booked_deal_count,
            coalesce(
                targets.total_churned_contraction_deal_count, 0
            ) as total_churned_contraction_deal_count,
            coalesce(
                targets.total_pipe_generation_net_arr, 0
            ) as total_pipe_generation_net_arr,
            coalesce(
                targets.total_pipe_generation_deal_count, 0
            ) as total_pipe_generation_deal_count,
            coalesce(
                targets.total_created_and_booked_same_quarter_net_arr, 0
            ) as total_created_and_booked_same_quarter_net_arr,
            coalesce(
                targets.total_sao_generation_net_arr, 0
            ) as total_sao_generation_net_arr,
            coalesce(
                targets.total_sao_generation_deal_count, 0
            ) as total_sao_generation_deal_count,

            coalesce(targets.calculated_target_net_arr, 0) as calculated_target_net_arr,
            coalesce(
                targets.calculated_target_deal_count, 0
            ) as calculated_target_deal_count,
            coalesce(
                targets.calculated_target_pipe_generation, 0
            ) as calculated_target_pipe_generation,

            -- totals quarter plus 1
            coalesce(
                rq_plus_one.total_booked_net_arr, 0
            ) as rq_plus_1_total_booked_net_arr,
            coalesce(
                rq_plus_one.total_booked_deal_count, 0
            ) as rq_plus_1_total_booked_deal_count,
            coalesce(rq_plus_one.target_net_arr, 0) as rq_plus_1_target_net_arr,
            coalesce(rq_plus_one.target_deal_count, 0) as rq_plus_1_target_deal_count,
            coalesce(
                rq_plus_one.calculated_target_net_arr, 0
            ) as rq_plus_1_calculated_target_net_arr,
            coalesce(
                rq_plus_one.calculated_target_deal_count, 0
            ) as rq_plus_1_calculated_target_deal_count,

            -- totals quarter plus 2
            coalesce(
                rq_plus_two.total_booked_net_arr, 0
            ) as rq_plus_2_total_booked_net_arr,
            coalesce(
                rq_plus_two.total_booked_deal_count, 0
            ) as rq_plus_2_total_booked_deal_count,
            coalesce(rq_plus_two.target_net_arr, 0) as rq_plus_2_target_net_arr,
            coalesce(rq_plus_two.target_deal_count, 0) as rq_plus_2_target_deal_count,
            coalesce(
                rq_plus_two.calculated_target_net_arr, 0
            ) as rq_plus_2_calculated_target_net_arr,
            coalesce(
                rq_plus_two.calculated_target_deal_count, 0
            ) as rq_plus_2_calculated_target_deal_count,

            coalesce(qtd_target.qtd_target_net_arr, 0) as qtd_target_net_arr,
            coalesce(qtd_target.qtd_target_deal_count, 0) as qtd_target_deal_count,
            coalesce(
                qtd_target.qtd_target_pipe_generation_net_arr, 0
            ) as qtd_target_pipe_generation_net_arr,

            -- totals one year ago
            coalesce(
                year_minus_one.total_booked_net_arr, 0
            ) as minus_1_year_total_booked_net_arr,
            coalesce(
                year_minus_one.total_booked_deal_count, 0
            ) as minus_1_year_total_booked_deal_count,
            coalesce(
                year_minus_one.total_pipe_generation_net_arr, 0
            ) as minus_1_year_total_pipe_generation_net_arr,
            coalesce(
                year_minus_one.total_pipe_generation_deal_count, 0
            ) as minus_1_year_total_pipe_generation_deal_count,

            -- TIMESTAMP
            current_timestamp as dbt_last_run_at

        from base_fields base
        -- base keys dictionary
        left join
            agg_demo_keys
            on base.report_user_segment_geo_region_area_sqs_ot
            = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
        left join
            consolidated_metrics metrics
            on metrics.close_fiscal_quarter_date = base.close_fiscal_quarter_date
            and metrics.close_day_of_fiscal_quarter_normalised
            = base.close_day_of_fiscal_quarter_normalised
            and metrics.report_user_segment_geo_region_area_sqs_ot
            = base.report_user_segment_geo_region_area_sqs_ot
        -- current quarter
        left join
            consolidated_targets_per_day targets
            on targets.close_fiscal_quarter_date = base.close_fiscal_quarter_date
            and targets.close_day_of_fiscal_quarter_normalised
            = base.close_day_of_fiscal_quarter_normalised
            and targets.report_user_segment_geo_region_area_sqs_ot
            = base.report_user_segment_geo_region_area_sqs_ot
        -- quarter plus 1 targets
        left join
            consolidated_targets_per_day rq_plus_one
            on rq_plus_one.close_fiscal_quarter_date
            = base.rq_plus_1_close_fiscal_quarter_date
            and rq_plus_one.close_day_of_fiscal_quarter_normalised
            = base.close_day_of_fiscal_quarter_normalised
            and rq_plus_one.report_user_segment_geo_region_area_sqs_ot
            = base.report_user_segment_geo_region_area_sqs_ot
        -- quarter plus 2 targets
        left join
            consolidated_targets_per_day rq_plus_two
            on rq_plus_two.close_fiscal_quarter_date
            = base.rq_plus_2_close_fiscal_quarter_date
            and rq_plus_two.close_day_of_fiscal_quarter_normalised
            = base.close_day_of_fiscal_quarter_normalised
            and rq_plus_two.report_user_segment_geo_region_area_sqs_ot
            = base.report_user_segment_geo_region_area_sqs_ot
        -- qtd allocated targets
        left join
            funnel_allocated_targets_qtd qtd_target
            on qtd_target.close_fiscal_quarter_date = base.close_fiscal_quarter_date
            and qtd_target.close_day_of_fiscal_quarter_normalised
            = base.close_day_of_fiscal_quarter_normalised
            and qtd_target.report_user_segment_geo_region_area_sqs_ot
            = base.report_user_segment_geo_region_area_sqs_ot
        -- one year ago totals
        left join
            consolidated_targets_per_day year_minus_one
            on year_minus_one.close_fiscal_quarter_date
            = dateadd(month, -12, base.close_fiscal_quarter_date)
            and year_minus_one.close_day_of_fiscal_quarter_normalised
            = base.close_day_of_fiscal_quarter_normalised
            and year_minus_one.report_user_segment_geo_region_area_sqs_ot
            = base.report_user_segment_geo_region_area_sqs_ot



    )
select *
from final
