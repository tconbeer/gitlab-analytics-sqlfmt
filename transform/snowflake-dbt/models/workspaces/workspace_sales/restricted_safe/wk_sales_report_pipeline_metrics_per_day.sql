{{ config(alias="report_pipeline_metrics_day") }}

with
    date_details as (select * from {{ ref("wk_sales_date_details") }}),
    -- keys used for aggregated historical analysis
    agg_demo_keys as (select * from {{ ref("wk_sales_report_agg_demo_sqs_ot_keys") }}),
    sfdc_opportunity_xf as (

        select
            opties.*,
            today_date.date_actual as snapshot_date,
            today_date.day_of_fiscal_quarter_normalised
            as snapshot_day_of_fiscal_quarter_normalised,
            today_date.fiscal_quarter_name_fy as snapshot_fiscal_quarter_name,
            today_date.first_day_of_fiscal_quarter as snapshot_fiscal_quarter_date,
            today_date.fiscal_year as snapshot_fiscal_year,

            -- created within quarter net arr
            case
                when
                    opties.pipeline_created_fiscal_quarter_name
                    = today_date.fiscal_quarter_name_fy
                    and opties.is_eligible_created_pipeline_flag = 1
                then opties.net_arr
                else 0
            end as created_in_snapshot_quarter_net_arr,

            -- created within quarter deal count
            case
                when
                    opties.pipeline_created_fiscal_quarter_name
                    = today_date.fiscal_quarter_name_fy
                    and opties.is_eligible_created_pipeline_flag = 1
                then opties.calculated_deal_count
                else 0
            end as created_in_snapshot_quarter_deal_count

        from {{ ref("wk_sales_sfdc_opportunity_xf") }} opties
        cross join
            (select * from date_details where date_actual = current_date) today_date
        where
            opties.is_deleted = 0
            and opties.is_excluded_flag = 0
            and opties.is_edu_oss = 0
            and opties.net_arr is not null
            and lower(opties.deal_group) like any ('%growth%', '%new%')
            and opties.stage_name
            not
            in (
                '0-Pending Acceptance',
                'Unqualified',
                '00-Pre Opportunity',
                '9-Unqualified',
                '10-Duplicate'
            )
    -- Not JiHu account
    ),
    sfdc_opportunity_snapshot_history_xf as (

        select opp_snapshot.*
        from {{ ref("wk_sales_sfdc_opportunity_snapshot_history_xf") }} opp_snapshot
        where
            opp_snapshot.is_deleted = 0
            and opp_snapshot.is_excluded_flag = 0
            and opp_snapshot.is_edu_oss = 0
            and opp_snapshot.net_arr is not null
            and lower(opp_snapshot.deal_group) like any ('%growth%', '%new%')
            -- include up to current date, where we use the current opportunity object
            and opp_snapshot.snapshot_date < current_date
            -- stage 1 plus, won & lost excluded ommited deals    
            and opp_snapshot.stage_name
            not
            in (
                '0-Pending Acceptance',
                'Unqualified',
                '00-Pre Opportunity',
                '9-Unqualified',
                '10-Duplicate'
            )
    -- Not JiHu account
    ),
    pipeline_snapshot as (

        select
            -- -----------------------------------
            -- report keys
            opp_snapshot.report_user_segment_geo_region_area_sqs_ot,

            -- -----------------------------------
            -- ---------------------------------------------------------------------------------
            -- snapshot date fields
            opp_snapshot.snapshot_date,
            opp_snapshot.snapshot_fiscal_year,
            opp_snapshot.snapshot_fiscal_quarter_name,
            opp_snapshot.snapshot_fiscal_quarter_date,
            opp_snapshot.snapshot_day_of_fiscal_quarter_normalised,
            -- ---------------------------------------------------------------------------------
            opp_snapshot.stage_name,
            opp_snapshot.forecast_category_name,
            opp_snapshot.is_renewal,
            opp_snapshot.is_won,
            opp_snapshot.is_lost,
            opp_snapshot.is_open,
            opp_snapshot.is_excluded_flag,

            opp_snapshot.close_fiscal_quarter_name,
            opp_snapshot.close_fiscal_quarter_date,
            opp_snapshot.created_fiscal_quarter_name,
            opp_snapshot.created_fiscal_quarter_date,

            opp_snapshot.net_arr,

            opp_snapshot.calculated_deal_count as deal_count,

            opp_snapshot.open_1plus_deal_count,
            opp_snapshot.open_3plus_deal_count,
            opp_snapshot.open_4plus_deal_count,

            -- booked deal count
            opp_snapshot.booked_deal_count,
            opp_snapshot.churned_contraction_deal_count,

            -- ---------------------------------------------------------------------------------
            -- NF: 20210201  NET ARR fields
            opp_snapshot.open_1plus_net_arr,
            opp_snapshot.open_3plus_net_arr,
            opp_snapshot.open_4plus_net_arr,

            -- booked net _arr
            opp_snapshot.booked_net_arr,

            -- churned net_arr
            opp_snapshot.churned_contraction_net_arr,

            opp_snapshot.created_and_won_same_quarter_net_arr,
            opp_snapshot.created_in_snapshot_quarter_net_arr,
            opp_snapshot.created_in_snapshot_quarter_deal_count

        from sfdc_opportunity_snapshot_history_xf opp_snapshot
        -- Keep the UNION ALL, somehow UNION is losing data
        union all
        select
            -- -----------------------------------
            -- report keys
            opties.report_user_segment_geo_region_area_sqs_ot,
            -- -----------------------------------
            -- ---------------------------------------------------------------------------------
            -- snapshot date fields
            opties.snapshot_date,
            opties.snapshot_fiscal_year,
            opties.snapshot_fiscal_quarter_name,
            opties.snapshot_fiscal_quarter_date,
            opties.snapshot_day_of_fiscal_quarter_normalised,
            -- ---------------------------------------------------------------------------------
            opties.stage_name,
            opties.forecast_category_name,
            opties.is_renewal,
            opties.is_won,
            opties.is_lost,
            opties.is_open,

            opties.is_excluded_flag,

            opties.close_fiscal_quarter_name,
            opties.close_fiscal_quarter_date,

            opties.created_fiscal_quarter_name,
            opties.created_fiscal_quarter_date,

            opties.net_arr,

            opties.calculated_deal_count as deal_count,

            opties.open_1plus_deal_count,
            opties.open_3plus_deal_count,
            opties.open_4plus_deal_count,

            -- booked deal count
            opties.booked_deal_count,
            opties.churned_contraction_deal_count,

            -- ---------------------------------------------------------------------------------
            -- NF: 20210201  NET ARR fields
            opties.open_1plus_net_arr,
            opties.open_3plus_net_arr,
            opties.open_4plus_net_arr,

            -- booked net _arr
            opties.booked_net_arr,

            -- churned net_arr
            opties.churned_contraction_net_arr,

            -- created and closed within the quarter net arr
            opties.created_and_won_same_quarter_net_arr,

            -- created within quarter
            opties.created_in_snapshot_quarter_net_arr,

            -- created within quarter
            opties.created_in_snapshot_quarter_deal_count

        from sfdc_opportunity_xf opties

    ),
    reported_quarter as (

        -- daily snapshot of pipeline metrics per quarter within the quarter
        select
            pipeline_snapshot.snapshot_fiscal_quarter_date as close_fiscal_quarter_date,
            pipeline_snapshot.snapshot_day_of_fiscal_quarter_normalised
            as close_day_of_fiscal_quarter_normalised,

            -- -----------------
            -- report keys
            -- FY23 needs to be updated to the new logic
            pipeline_snapshot.report_user_segment_geo_region_area_sqs_ot,
            -- -----------------
            sum(pipeline_snapshot.deal_count) as deal_count,
            sum(pipeline_snapshot.booked_deal_count) as booked_deal_count,
            sum(
                pipeline_snapshot.churned_contraction_deal_count
            ) as churned_contraction_deal_count,

            sum(pipeline_snapshot.open_1plus_deal_count) as open_1plus_deal_count,
            sum(pipeline_snapshot.open_3plus_deal_count) as open_3plus_deal_count,
            sum(pipeline_snapshot.open_4plus_deal_count) as open_4plus_deal_count,

            -- ---------------------------------------------------------------------------------
            -- NF: 20210201  NET ARR fields
            sum(pipeline_snapshot.open_1plus_net_arr) as open_1plus_net_arr,
            sum(pipeline_snapshot.open_3plus_net_arr) as open_3plus_net_arr,
            sum(pipeline_snapshot.open_4plus_net_arr) as open_4plus_net_arr,
            sum(pipeline_snapshot.booked_net_arr) as booked_net_arr,

            -- churned net_arr
            sum(
                pipeline_snapshot.churned_contraction_net_arr
            ) as churned_contraction_net_arr,

            sum(
                pipeline_snapshot.created_and_won_same_quarter_net_arr
            ) as created_and_won_same_quarter_net_arr

        -- ---------------------------------------------------------------------------------
        from pipeline_snapshot
        -- snapshot quarter rows that close within the same quarter
        where
            pipeline_snapshot.snapshot_fiscal_quarter_name
            = pipeline_snapshot.close_fiscal_quarter_name
        group by 1, 2, 3

    -- Quarter plus 1, from the reported quarter perspective
    ),
    report_quarter_plus_1 as (

        select
            pipeline_snapshot.snapshot_fiscal_quarter_date as close_fiscal_quarter_date,
            pipeline_snapshot.snapshot_day_of_fiscal_quarter_normalised
            as close_day_of_fiscal_quarter_normalised,

            pipeline_snapshot.close_fiscal_quarter_name
            as rq_plus_1_close_fiscal_quarter_name,
            pipeline_snapshot.close_fiscal_quarter_date
            as rq_plus_1_close_fiscal_quarter_date,

            -- -----------------
            -- report keys
            pipeline_snapshot.report_user_segment_geo_region_area_sqs_ot,
            -- -----------------
            sum(
                pipeline_snapshot.open_1plus_deal_count
            ) as rq_plus_1_open_1plus_deal_count,
            sum(
                pipeline_snapshot.open_3plus_deal_count
            ) as rq_plus_1_open_3plus_deal_count,
            sum(
                pipeline_snapshot.open_4plus_deal_count
            ) as rq_plus_1_open_4plus_deal_count,

            -- ----------------------------
            -- Net ARR 
            sum(pipeline_snapshot.open_1plus_net_arr) as rq_plus_1_open_1plus_net_arr,
            sum(pipeline_snapshot.open_3plus_net_arr) as rq_plus_1_open_3plus_net_arr,
            sum(pipeline_snapshot.open_4plus_net_arr) as rq_plus_1_open_4plus_net_arr

        from pipeline_snapshot
        -- restrict the report to show rows in quarter plus 1 of snapshot quarter
        where
            pipeline_snapshot.snapshot_fiscal_quarter_date
            = dateadd(month, -3, pipeline_snapshot.close_fiscal_quarter_date)
            -- exclude lost deals from pipeline
            and pipeline_snapshot.is_lost = 0
        group by 1, 2, 3, 4, 5

    -- Quarter plus 2, from the reported quarter perspective
    ),
    report_quarter_plus_2 as (

        select
            pipeline_snapshot.snapshot_fiscal_quarter_date as close_fiscal_quarter_date,
            pipeline_snapshot.snapshot_day_of_fiscal_quarter_normalised
            as close_day_of_fiscal_quarter_normalised,

            pipeline_snapshot.close_fiscal_quarter_name
            as rq_plus_2_close_fiscal_quarter_name,
            pipeline_snapshot.close_fiscal_quarter_date
            as rq_plus_2_close_fiscal_quarter_date,

            -- -----------------
            -- report keys
            pipeline_snapshot.report_user_segment_geo_region_area_sqs_ot,
            -- -----------------
            sum(
                pipeline_snapshot.open_1plus_deal_count
            ) as rq_plus_2_open_1plus_deal_count,
            sum(
                pipeline_snapshot.open_3plus_deal_count
            ) as rq_plus_2_open_3plus_deal_count,
            sum(
                pipeline_snapshot.open_4plus_deal_count
            ) as rq_plus_2_open_4plus_deal_count,

            -- -----------------
            -- Net ARR 
            -- Use Net ARR instead
            sum(pipeline_snapshot.open_1plus_net_arr) as rq_plus_2_open_1plus_net_arr,
            sum(pipeline_snapshot.open_3plus_net_arr) as rq_plus_2_open_3plus_net_arr,
            sum(pipeline_snapshot.open_4plus_net_arr) as rq_plus_2_open_4plus_net_arr

        from pipeline_snapshot
        -- restrict the report to show rows in quarter plus 2 of snapshot quarter
        where
            pipeline_snapshot.snapshot_fiscal_quarter_date
            = dateadd(month, -6, pipeline_snapshot.close_fiscal_quarter_date)
            -- exclude lost deals from pipeline
            and pipeline_snapshot.is_lost = 0
        group by 1, 2, 3, 4, 5

    ),
    pipeline_gen as (

        select
            opp_history.snapshot_fiscal_quarter_date as close_fiscal_quarter_date,
            opp_history.snapshot_day_of_fiscal_quarter_normalised
            as close_day_of_fiscal_quarter_normalised,

            -- -----------------
            -- report keys
            opp_history.report_user_segment_geo_region_area_sqs_ot,
            -- -----------------
            sum(opp_history.created_in_snapshot_quarter_deal_count) as pipe_gen_count,

            -- Net ARR 
            sum(opp_history.created_in_snapshot_quarter_net_arr) as pipe_gen_net_arr

        from sfdc_opportunity_snapshot_history_xf opp_history
        -- restrict the rows to pipeline created on the quarter of the snapshot
        where
            opp_history.snapshot_fiscal_quarter_name
            = opp_history.pipeline_created_fiscal_quarter_name
            and opp_history.is_eligible_created_pipeline_flag = 1
        group by 1, 2, 3
        -- Keep the UNION ALL, somehow UNION is losing data
        union all
        select
            opties.snapshot_fiscal_quarter_date as close_fiscal_quarter_date,
            opties.snapshot_day_of_fiscal_quarter_normalised
            as close_day_of_fiscal_quarter_normalised,

            -- -----------------
            -- report keys
            opties.report_user_segment_geo_region_area_sqs_ot,
            -- -----------------
            sum(opties.created_in_snapshot_quarter_deal_count) as pipe_gen_count,

            -- Net ARR 
            sum(opties.created_in_snapshot_quarter_net_arr) as pipe_gen_net_arr

        from sfdc_opportunity_xf opties
        -- restrict the rows to pipeline created on the quarter of the snapshot
        where
            opties.snapshot_fiscal_quarter_name
            = opties.pipeline_created_fiscal_quarter_name
            and opties.is_eligible_created_pipeline_flag = 1
        group by 1, 2, 3

    -- Sales Accepted Opportunities
    ),
    sao_gen as (

        select
            opp_history.snapshot_fiscal_quarter_date as close_fiscal_quarter_date,
            opp_history.snapshot_day_of_fiscal_quarter_normalised
            as close_day_of_fiscal_quarter_normalised,

            -- -----------------
            -- report keys
            opp_history.report_user_segment_geo_region_area_sqs_ot,
            -- -----------------
            sum(opp_history.calculated_deal_count) as sao_deal_count,

            -- Net ARR 
            sum(opp_history.net_arr) as sao_net_arr

        from sfdc_opportunity_snapshot_history_xf opp_history
        -- restrict the rows to pipeline created on the quarter of the snapshot
        where
            opp_history.snapshot_fiscal_quarter_name
            = opp_history.sales_accepted_fiscal_quarter_name
            and opp_history.is_eligible_sao_flag = 1
        group by 1, 2, 3
        -- Keep the UNION ALL, somehow UNION is losing data
        union all
        select
            opties.snapshot_fiscal_quarter_date as close_fiscal_quarter_date,
            opties.snapshot_day_of_fiscal_quarter_normalised
            as close_day_of_fiscal_quarter_normalised,

            -- -----------------
            -- report keys
            opties.report_user_segment_geo_region_area_sqs_ot,
            -- -----------------
            sum(opties.calculated_deal_count) as sao_deal_count,

            -- Net ARR 
            sum(opties.net_arr) as sao_net_arr

        from sfdc_opportunity_xf opties
        -- restrict the rows to pipeline created on the quarter of the snapshot
        where
            opties.snapshot_fiscal_quarter_name
            = opties.sales_accepted_fiscal_quarter_name
            and opties.is_eligible_sao_flag = 1
        group by 1, 2, 3

    -- These CTE builds a complete set of values 
    ),
    key_fields as (

        select report_user_segment_geo_region_area_sqs_ot, close_fiscal_quarter_date
        from reported_quarter
        union
        select report_user_segment_geo_region_area_sqs_ot, close_fiscal_quarter_date
        from report_quarter_plus_1
        union
        select report_user_segment_geo_region_area_sqs_ot, close_fiscal_quarter_date
        from report_quarter_plus_2
        union
        select report_user_segment_geo_region_area_sqs_ot, close_fiscal_quarter_date
        from pipeline_gen
        union
        select report_user_segment_geo_region_area_sqs_ot, close_fiscal_quarter_date
        from sao_gen

    ),
    base_fields as (

        select
            key_fields.*,
            close_date.fiscal_quarter_name_fy as close_fiscal_quarter_name,
            close_date.date_actual as close_date,
            close_date.day_of_fiscal_quarter_normalised
            as close_day_of_fiscal_quarter_normalised,
            close_date.fiscal_year as close_fiscal_year,
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
    report_pipeline_metrics_day as (

        select
            -- ---------------------------
            -- keys
            base_fields.report_user_segment_geo_region_area_sqs_ot,

            base_fields.close_fiscal_quarter_date,
            base_fields.close_fiscal_quarter_name,
            base_fields.close_fiscal_year,
            base_fields.close_date,
            base_fields.close_day_of_fiscal_quarter_normalised,
            -- ---------------------------
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

            -- used to track the latest updated day in the model
            -- this might be different to the latest available information in the
            -- source models
            -- as dbt runs are not necesarly in synch
            case
                when base_fields.close_date = current_date
                then 1
                else 0
            end as is_today_flag,

            -- report quarter plus 1 / 2 date fields
            base_fields.rq_plus_1_close_fiscal_quarter_name,
            base_fields.rq_plus_1_close_fiscal_quarter_date,
            base_fields.rq_plus_2_close_fiscal_quarter_name,
            base_fields.rq_plus_2_close_fiscal_quarter_date,

            -- reported quarter
            coalesce(reported_quarter.deal_count, 0) as deal_count,
            coalesce(
                reported_quarter.open_1plus_deal_count, 0
            ) as open_1plus_deal_count,
            coalesce(
                reported_quarter.open_3plus_deal_count, 0
            ) as open_3plus_deal_count,
            coalesce(
                reported_quarter.open_4plus_deal_count, 0
            ) as open_4plus_deal_count,
            coalesce(reported_quarter.booked_deal_count, 0) as booked_deal_count,
            -- churned deal count
            coalesce(
                reported_quarter.churned_contraction_deal_count,
                0
            ) as churned_contraction_deal_count,



            -- reported quarter + 1
            coalesce(
                report_quarter_plus_1.rq_plus_1_open_1plus_deal_count, 0
            ) as rq_plus_1_open_1plus_deal_count,
            coalesce(
                report_quarter_plus_1.rq_plus_1_open_3plus_deal_count, 0
            ) as rq_plus_1_open_3plus_deal_count,
            coalesce(
                report_quarter_plus_1.rq_plus_1_open_4plus_deal_count,
                0
            ) as rq_plus_1_open_4plus_deal_count,

            -- reported quarter + 2
            coalesce(
                report_quarter_plus_2.rq_plus_2_open_1plus_deal_count, 0
            ) as rq_plus_2_open_1plus_deal_count,
            coalesce(
                report_quarter_plus_2.rq_plus_2_open_3plus_deal_count, 0
            ) as rq_plus_2_open_3plus_deal_count,
            coalesce(
                report_quarter_plus_2.rq_plus_2_open_4plus_deal_count,
                0
            ) as rq_plus_2_open_4plus_deal_count,

            -- ----------------------------
            -- Net ARR 
            -- Use Net ARR instead     
            -- created and closed
            -- reported quarter
            coalesce(reported_quarter.booked_net_arr, 0) as booked_net_arr,
            -- churned net_arr
            coalesce(
                reported_quarter.churned_contraction_net_arr, 0
            ) as churned_contraction_net_arr,
            coalesce(reported_quarter.open_1plus_net_arr, 0) as open_1plus_net_arr,
            coalesce(reported_quarter.open_3plus_net_arr, 0) as open_3plus_net_arr,
            coalesce(reported_quarter.open_4plus_net_arr, 0) as open_4plus_net_arr,

            coalesce(
                reported_quarter.created_and_won_same_quarter_net_arr,
                0
            ) as created_and_won_same_quarter_net_arr,


            -- reported quarter + 1
            coalesce(
                report_quarter_plus_1.rq_plus_1_open_1plus_net_arr, 0
            ) as rq_plus_1_open_1plus_net_arr,
            coalesce(
                report_quarter_plus_1.rq_plus_1_open_3plus_net_arr, 0
            ) as rq_plus_1_open_3plus_net_arr,
            coalesce(
                report_quarter_plus_1.rq_plus_1_open_4plus_net_arr,
                0
            ) as rq_plus_1_open_4plus_net_arr,

            -- reported quarter + 2
            coalesce(
                report_quarter_plus_2.rq_plus_2_open_1plus_net_arr, 0
            ) as rq_plus_2_open_1plus_net_arr,
            coalesce(
                report_quarter_plus_2.rq_plus_2_open_3plus_net_arr, 0
            ) as rq_plus_2_open_3plus_net_arr,
            coalesce(
                report_quarter_plus_2.rq_plus_2_open_4plus_net_arr,
                0
            ) as rq_plus_2_open_4plus_net_arr,

            -- pipe gen
            coalesce(pipeline_gen.pipe_gen_count, 0) as pipe_gen_count,
            coalesce(pipeline_gen.pipe_gen_net_arr, 0) as pipe_gen_net_arr,

            -- sao gen
            coalesce(sao_gen.sao_deal_count, 0) as sao_deal_count,
            coalesce(sao_gen.sao_net_arr, 0) as sao_net_arr,

            -- one year ago sao gen
            coalesce(minus_1_year_sao_gen.sao_net_arr, 0) as minus_1_year_sao_net_arr,
            coalesce(
                minus_1_year_sao_gen.sao_deal_count, 0) as minus_1_year_sao_deal_count,

            -- one year ago pipe gen
            coalesce(
                minus_1_year_pipe_gen.pipe_gen_net_arr, 0
            ) as minus_1_year_pipe_gen_net_arr,
            coalesce(
                minus_1_year_pipe_gen.pipe_gen_count,
                0
            ) as minus_1_year_pipe_gen_deal_count,

            -- TIMESTAMP
            current_timestamp as dbt_last_run_at

        -- created a list of all options to avoid having blanks when attaching metrics
        from base_fields
        -- base keys dictionary
        left join
            agg_demo_keys
            on base_fields.report_user_segment_geo_region_area_sqs_ot
            = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
        -- historical quarter
        left join
            reported_quarter
            on base_fields.close_day_of_fiscal_quarter_normalised
            = reported_quarter.close_day_of_fiscal_quarter_normalised
            and base_fields.close_fiscal_quarter_date
            = reported_quarter.close_fiscal_quarter_date
            and base_fields.report_user_segment_geo_region_area_sqs_ot
            = reported_quarter.report_user_segment_geo_region_area_sqs_ot
        -- next quarter in relation to the considered reported quarter
        left join
            report_quarter_plus_1
            on base_fields.close_day_of_fiscal_quarter_normalised
            = report_quarter_plus_1.close_day_of_fiscal_quarter_normalised
            and base_fields.close_fiscal_quarter_date
            = report_quarter_plus_1.close_fiscal_quarter_date
            and base_fields.report_user_segment_geo_region_area_sqs_ot
            = report_quarter_plus_1.report_user_segment_geo_region_area_sqs_ot
        -- 2 quarters ahead in relation to the considered reported quarter
        left join
            report_quarter_plus_2
            on base_fields.close_day_of_fiscal_quarter_normalised
            = report_quarter_plus_2.close_day_of_fiscal_quarter_normalised
            and base_fields.close_fiscal_quarter_date
            = report_quarter_plus_2.close_fiscal_quarter_date
            and base_fields.report_user_segment_geo_region_area_sqs_ot
            = report_quarter_plus_2.report_user_segment_geo_region_area_sqs_ot
        -- Pipe generation piece
        left join
            pipeline_gen
            on base_fields.close_day_of_fiscal_quarter_normalised
            = pipeline_gen.close_day_of_fiscal_quarter_normalised
            and base_fields.close_fiscal_quarter_date
            = pipeline_gen.close_fiscal_quarter_date
            and base_fields.report_user_segment_geo_region_area_sqs_ot
            = pipeline_gen.report_user_segment_geo_region_area_sqs_ot
        -- Sales Accepted Opportunity Generation
        left join
            sao_gen
            on base_fields.close_day_of_fiscal_quarter_normalised
            = sao_gen.close_day_of_fiscal_quarter_normalised
            and base_fields.close_fiscal_quarter_date
            = sao_gen.close_fiscal_quarter_date
            and base_fields.report_user_segment_geo_region_area_sqs_ot
            = sao_gen.report_user_segment_geo_region_area_sqs_ot
        -- One Year Ago  pipeline generation
        left join
            pipeline_gen minus_1_year_pipe_gen
            on minus_1_year_pipe_gen.close_day_of_fiscal_quarter_normalised
            = base_fields.close_day_of_fiscal_quarter_normalised
            and minus_1_year_pipe_gen.close_fiscal_quarter_date
            = dateadd(month, -12, base_fields.close_fiscal_quarter_date)
            and minus_1_year_pipe_gen.report_user_segment_geo_region_area_sqs_ot
            = base_fields.report_user_segment_geo_region_area_sqs_ot
        -- One Year Ago Sales Accepted Opportunity Generation
        left join
            sao_gen minus_1_year_sao_gen
            on minus_1_year_sao_gen.close_day_of_fiscal_quarter_normalised
            = base_fields.close_day_of_fiscal_quarter_normalised
            and minus_1_year_sao_gen.close_fiscal_quarter_date
            = dateadd(month, -12, base_fields.close_fiscal_quarter_date)
            and minus_1_year_sao_gen.report_user_segment_geo_region_area_sqs_ot
            = base_fields.report_user_segment_geo_region_area_sqs_ot

    )

select *
from report_pipeline_metrics_day
