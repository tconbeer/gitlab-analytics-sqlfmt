{{ config(alias="report_pipeline_movement_daily") }}

with
    date_details as (select * from {{ ref("wk_sales_date_details") }}),
    report_pipeline_movement_quarter as (

        select * from {{ ref("wk_sales_report_pipeline_movement_quarter") }}

    ),
    sfdc_opportunity_snapshot_history_xf as (

        select *
        from {{ ref("wk_sales_sfdc_opportunity_snapshot_history_xf") }}
        where is_deleted = 0 and is_edu_oss = 0

    ),
    target_day as (

        select
            day_of_fiscal_quarter_normalised as current_day_of_fiscal_quarter_normalised
        from date_details
        where date_actual = current_date

    ),
    report_period as (

        select
            date_actual as report_date,
            day_of_fiscal_quarter_normalised as report_day_of_fiscal_quarter_normalised,
            fiscal_quarter_name_fy as report_fiscal_quarter_name,
            first_day_of_fiscal_quarter as report_fiscal_quarter_date
        from date_details

    -- using the daily perspective and the max, min and resolution dates from the
    -- quarterly view
    -- it is possible to reconstruct a daily changes perspective
    ),
    daily_pipeline_changes as (

        select
            report.report_date,
            report.report_day_of_fiscal_quarter_normalised,
            report.report_fiscal_quarter_name,
            report.report_fiscal_quarter_date,

            pipe.opportunity_id,
            pipe.min_snapshot_date,
            pipe.max_snapshot_date,

            pipe.quarter_start_net_arr,
            pipe.quarter_end_net_arr,
            pipe.last_day_net_arr,
            pipe.pipe_resolution_date,

            case
                when report.report_date >= pipe.pipe_resolution_date
                then pipe.pipe_resolution
                when
                    report.report_date >= pipe.min_snapshot_date
                    and report.report_date < pipe.pipe_resolution_date
                then '7. Open'
                else null
            end as pipe_resolution,

            pipe.pipeline_type

        from report_period report
        inner join
            report_pipeline_movement_quarter pipe
            on pipe.report_fiscal_quarter_date = report.report_fiscal_quarter_date

    ),
    report_pipeline_movemnet_daily as (

        select
            pipe.*,
            opp_snap.stage_name,
            opp_snap.close_date,
            opp_snap.forecast_category_name,
            -- before the deal was to close in quarter we show null
            -- within start and resolution the net arr of that day
            -- after pipe resolution the value the opty had that last day
            case
                when pipe.report_date >= pipe.pipe_resolution_date
                then pipe.last_day_net_arr
                when
                    pipe.report_date >= pipe.min_snapshot_date
                    and pipe.report_date < pipe.pipe_resolution_date
                then opp_snap.net_arr
                else null
            end as net_arr
        from daily_pipeline_changes pipe
        inner join
            sfdc_opportunity_snapshot_history_xf opp_snap
            on pipe.opportunity_id = opp_snap.opportunity_id
            and pipe.report_date = opp_snap.snapshot_date
        where pipe.pipe_resolution is not null

    )

select *
from report_pipeline_movemnet_daily
