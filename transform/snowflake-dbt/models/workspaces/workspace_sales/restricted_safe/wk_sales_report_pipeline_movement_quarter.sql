{{ config(alias="report_pipeline_movement_quarter") }}

with
    sfdc_opportunity_snapshot_history_xf as (

        select *
        from {{ ref("wk_sales_sfdc_opportunity_snapshot_history_xf") }}
        where is_deleted = 0 and is_edu_oss = 0

    ),
    sfdc_opportunity_xf as (

        select
            opportunity_id,
            close_fiscal_quarter_date,
            stage_name,
            is_won,
            is_lost,
            is_open,
            is_renewal,
            order_type_stamped,
            sales_qualified_source,
            deal_category,
            deal_group,
            opportunity_category,
            sales_team_cro_level,
            sales_team_rd_asm_level
        from {{ ref("wk_sales_sfdc_opportunity_xf") }}
        where is_deleted = 0 and is_edu_oss = 0

    ),
    today_date as (

        select distinct
            first_day_of_fiscal_quarter as current_fiscal_quarter_date,
            fiscal_quarter_name_fy as current_fiscal_quarter_name,
            90 - datediff(
                day, date_actual, last_day_of_fiscal_quarter
            ) as current_day_of_fiscal_quarter_normalised
        from {{ ref("wk_sales_date_details") }}
        where date_actual = current_date

    ),
    pipeline_type_start_ids as (

        select
            opportunity_id,
            snapshot_fiscal_quarter_date,
            max(
                snapshot_day_of_fiscal_quarter_normalised
            ) as max_snapshot_day_of_fiscal_quarter_normalised,
            min(
                snapshot_day_of_fiscal_quarter_normalised
            ) as min_snapshot_day_of_fiscal_quarter_normalised
        from sfdc_opportunity_snapshot_history_xf
        where  -- closing in the same quarter of the snapshot
            snapshot_fiscal_quarter_date = close_fiscal_quarter_date
            and stage_name not in (
                '9-Unqualified',
                '10-Duplicate',
                'Unqualified',
                '00-Pre Opportunity',
                '0-Pending Acceptance'
            )
            and snapshot_day_of_fiscal_quarter_normalised <= 5
            -- exclude web direct purchases
            and is_web_portal_purchase = 0
        group by 1, 2

    ),
    pipeline_type_web_purchase_ids as (

        select
            opportunity_id,
            snapshot_fiscal_quarter_date,
            max(
                snapshot_day_of_fiscal_quarter_normalised
            ) as max_snapshot_day_of_fiscal_quarter_normalised,
            min(
                snapshot_day_of_fiscal_quarter_normalised
            ) as min_snapshot_day_of_fiscal_quarter_normalised
        from sfdc_opportunity_snapshot_history_xf
        where  -- closing in the same quarter of the snapshot
            snapshot_fiscal_quarter_date = close_fiscal_quarter_date
            and stage_name not in (
                '9-Unqualified',
                '10-Duplicate',
                'Unqualified',
                '00-Pre Opportunity',
                '0-Pending Acceptance'
            )
            -- include web direct purchases
            and is_web_portal_purchase = 1
        group by 1, 2

    ),
    pipeline_type_created_ids as (

        select
            created.opportunity_id,
            created.pipeline_created_fiscal_quarter_date,
            min(
                created.snapshot_day_of_fiscal_quarter_normalised
            ) as snapshot_day_of_fiscal_quarter_normalised
        from sfdc_opportunity_snapshot_history_xf created
        where
            created.stage_name not in (
                '9-Unqualified',
                '10-Duplicate',
                'Unqualified',
                '00-Pre Opportunity',
                '0-Pending Acceptance'
            )
            -- pipeline created same quarter
            and created.snapshot_fiscal_quarter_date
            = created.pipeline_created_fiscal_quarter_date
            -- created and landed
            and created.pipeline_created_fiscal_quarter_date
            = created.close_fiscal_quarter_date
            and created.is_eligible_created_pipeline_flag = 1
        group by 1, 2

    ),
    pipeline_type_quarter_start as (

        -- create a list of opties and min snapshot day to identify all the opties
        -- that should be flagged as starting in the first 5 days
        select
            starting.opportunity_id,
            starting.snapshot_fiscal_quarter_date,
            starting.close_fiscal_quarter_date as starting_close_fiscal_quarter_date,
            starting.close_date as starting_close_date,
            starting.forecast_category_name as starting_forecast_category,
            starting.net_arr as starting_net_arr,
            starting.booked_net_arr as starting_booked_net_arr,
            starting.stage_name as starting_stage,
            starting.snapshot_date as starting_snapshot_date,
            starting.is_won as starting_is_won,
            starting.is_open as starting_is_open,
            starting.is_lost as starting_is_lost
        from sfdc_opportunity_snapshot_history_xf starting
        inner join
            pipeline_type_start_ids starting_list
            on starting.opportunity_id = starting_list.opportunity_id
            and starting.snapshot_fiscal_quarter_date
            = starting_list.snapshot_fiscal_quarter_date
            and starting.snapshot_day_of_fiscal_quarter_normalised
            = starting_list.max_snapshot_day_of_fiscal_quarter_normalised
        where  -- closing in the same quarter of the snapshot
            starting.snapshot_fiscal_quarter_date = starting.close_fiscal_quarter_date
            -- exclude deals that were before day 5, unless they were at day 5
            and starting_list.max_snapshot_day_of_fiscal_quarter_normalised = 5

    ),
    pipeline_type_quarter_created as (

        select
            created.opportunity_id,
            created.pipeline_created_fiscal_quarter_date,
            created.snapshot_date as created_snapshot_date,
            created.is_won as created_is_won,
            created.is_lost as created_is_lost,
            created.is_open as created_is_open
        from sfdc_opportunity_snapshot_history_xf created
        inner join
            pipeline_type_created_ids created_ids
            on created_ids.opportunity_id = created.opportunity_id
            and created_ids.pipeline_created_fiscal_quarter_date
            = created.pipeline_created_fiscal_quarter_date
            and created_ids.snapshot_day_of_fiscal_quarter_normalised
            = created.snapshot_day_of_fiscal_quarter_normalised
        left join
            pipeline_type_start_ids starting
            on starting.opportunity_id = created.opportunity_id
            and starting.snapshot_fiscal_quarter_date
            = created.snapshot_fiscal_quarter_date
        left join
            pipeline_type_web_purchase_ids web
            on web.opportunity_id = created.opportunity_id
            and web.snapshot_fiscal_quarter_date = created.snapshot_fiscal_quarter_date
        where
            created.stage_name not in (
                '9-Unqualified',
                '10-Duplicate',
                'Unqualified',
                '00-Pre Opportunity',
                '0-Pending Acceptance'
            )
            -- pipeline created same quarter
            and created.snapshot_fiscal_quarter_date
            = created.pipeline_created_fiscal_quarter_date
            and created.is_eligible_created_pipeline_flag = 1
            and created.pipeline_created_fiscal_quarter_date
            = created.close_fiscal_quarter_date
            -- not already flagged as starting pipeline
            and starting.opportunity_id is null
            and web.opportunity_id is null

    ),
    pipeline_type_pulled_in as (

        select
            pull.opportunity_id,
            pull.snapshot_fiscal_quarter_date,
            min(pull.snapshot_date) as pulled_in_snapshot_date
        from sfdc_opportunity_snapshot_history_xf pull
        left join
            pipeline_type_quarter_start pipe_start
            on pipe_start.opportunity_id = pull.opportunity_id
            and pipe_start.snapshot_fiscal_quarter_date
            = pull.snapshot_fiscal_quarter_date
        left join
            pipeline_type_quarter_created pipe_created
            on pipe_created.opportunity_id = pull.opportunity_id
            and pipe_created.pipeline_created_fiscal_quarter_date
            = pull.snapshot_fiscal_quarter_date
        left join
            pipeline_type_web_purchase_ids web
            on web.opportunity_id = pull.opportunity_id
            and web.snapshot_fiscal_quarter_date = pull.snapshot_fiscal_quarter_date
        where
            pull.stage_name not in (
                '9-Unqualified',
                '10-Duplicate',
                'Unqualified',
                '00-Pre Opportunity',
                '0-Pending Acceptance'
            )
            and pull.snapshot_fiscal_quarter_date = pull.close_fiscal_quarter_date
            and pipe_start.opportunity_id is null
            and pipe_created.opportunity_id is null
            and web.opportunity_id is null
        group by 1, 2

    ),
    pipeline_type_quarter_end as (

        select
            opportunity_id,
            snapshot_fiscal_quarter_date,
            close_fiscal_quarter_date as end_close_fiscal_quarter_date,
            close_date as end_close_date,
            forecast_category_name as end_forecast_category,
            net_arr as end_net_arr,
            booked_net_arr as end_booked_net_arr,
            stage_name as end_stage,
            stage_category as end_stage_category,
            is_won as end_is_won,
            is_open as end_is_open,
            is_lost as end_is_lost
        from sfdc_opportunity_snapshot_history_xf
        where
            (
                snapshot_day_of_fiscal_quarter_normalised = 90
                or snapshot_date = current_date
            )

    ),
    pipeline_type as (

        select
            opp_snap.opportunity_id,
            opp_snap.close_fiscal_quarter_date,
            opp_snap.close_fiscal_quarter_name,
            opp_snap.close_fiscal_year,

            pipe_start.starting_forecast_category,
            pipe_start.starting_net_arr,
            pipe_start.starting_stage,
            pipe_start.starting_close_date,
            pipe_start.starting_is_open,
            pipe_start.starting_is_won,
            pipe_start.starting_is_lost,

            pipe_end.end_forecast_category,
            pipe_end.end_net_arr,
            pipe_end.end_booked_net_arr,
            pipe_end.end_stage,
            pipe_end.end_stage_category,
            pipe_end.end_is_open,
            pipe_end.end_is_won,
            pipe_end.end_is_lost,
            pipe_end.end_close_date,

            pipe_created.created_is_won,
            pipe_created.created_is_lost,
            pipe_created.created_is_open,

            -- pipeline type, identifies if the opty was there at the begging of the
            -- quarter or not
            case
                when pipe_start.opportunity_id is not null
                then '1. Starting'
                when web.opportunity_id is not null
                then '4. Web Direct'
                when pipe_created.opportunity_id is not null
                then '2. Created & Landed'
                when pipe_pull.opportunity_id is not null
                then '3. Pulled in'
                else '0. Other'
            end as pipeline_type,

            -- created pipe
            max(pipe_created.created_snapshot_date) as pipeline_created_snapshot_date,

            max(
                case
                    when pipe_created.created_snapshot_date = opp_snap.snapshot_date
                    then opp_snap.net_arr
                    else null
                end
            ) as pipeline_created_net_arr,
            max(
                case
                    when pipe_created.created_snapshot_date = opp_snap.snapshot_date
                    then opp_snap.stage_name
                    else ''
                end
            ) as pipeline_created_stage,

            max(
                case
                    when pipe_created.created_snapshot_date = opp_snap.snapshot_date
                    then opp_snap.forecast_category_name
                    else ''
                end
            ) as pipeline_created_forecast_category,

            max(
                case
                    when pipe_created.created_snapshot_date = opp_snap.snapshot_date
                    then opp_snap.close_date
                    else null
                end
            ) as pipeline_created_close_date,

            -- pulled in pipe
            max(
                case
                    when pipe_pull.pulled_in_snapshot_date = opp_snap.snapshot_date
                    then opp_snap.net_arr
                    else null
                end
            ) as pipeline_pull_net_arr,

            -- --
            min(
                opp_snap.snapshot_day_of_fiscal_quarter_normalised
            ) as min_snapshot_day_of_fiscal_quarter_normalised,
            max(
                opp_snap.snapshot_day_of_fiscal_quarter_normalised
            ) as max_snapshot_day_of_fiscal_quarter_normalised,

            min(opp_snap.snapshot_date) as min_snapshot_date,
            max(opp_snap.snapshot_date) as max_snapshot_date,

            min(opp_snap.close_date) as min_close_date,
            max(opp_snap.close_date) as max_close_date,

            min(opp_snap.net_arr) as min_net_arr,
            max(opp_snap.net_arr) as max_net_arr,

            min(opp_snap.stage_name) as min_stage_name,
            max(opp_snap.stage_name) as max_stage_name
        from sfdc_opportunity_snapshot_history_xf opp_snap
        -- starting pipeline
        left join
            pipeline_type_quarter_start pipe_start
            on pipe_start.opportunity_id = opp_snap.opportunity_id
            and pipe_start.snapshot_fiscal_quarter_date
            = opp_snap.snapshot_fiscal_quarter_date
        -- end pipeline
        left join
            pipeline_type_quarter_end pipe_end
            on pipe_end.opportunity_id = opp_snap.opportunity_id
            and pipe_end.snapshot_fiscal_quarter_date
            = opp_snap.snapshot_fiscal_quarter_date
        -- created pipeline
        left join
            pipeline_type_quarter_created pipe_created
            on pipe_created.opportunity_id = opp_snap.opportunity_id
            and pipe_created.pipeline_created_fiscal_quarter_date
            = opp_snap.close_fiscal_quarter_date
        -- pulled in pipeline
        left join
            pipeline_type_pulled_in pipe_pull
            on pipe_pull.opportunity_id = opp_snap.opportunity_id
            and pipe_pull.snapshot_fiscal_quarter_date
            = opp_snap.snapshot_fiscal_quarter_date
        -- web direct pipeline
        left join
            pipeline_type_web_purchase_ids web
            on web.opportunity_id = opp_snap.opportunity_id
            and web.snapshot_fiscal_quarter_date = opp_snap.snapshot_fiscal_quarter_date
        -- closing in the same quarter of the snapshot
        where
            opp_snap.snapshot_fiscal_quarter_date = opp_snap.close_fiscal_quarter_date
            -- Exclude duplicate deals that were not created or started within the
            -- quarter
            and (
                pipe_start.opportunity_id is not null
                or pipe_created.opportunity_id is not null
                or pipe_pull.opportunity_id is not null
                or web.opportunity_id is not null
            )
        group by
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24

    -- last day within snapshot quarter of a particular opportunity
    ),
    pipeline_last_day_in_snapshot_quarter as (

        select
            pipeline_type.opportunity_id,
            pipeline_type.close_fiscal_quarter_date,
            history.stage_name,
            history.forecast_category_name,
            history.net_arr,
            history.booked_net_arr,
            history.is_won,
            history.is_lost,
            history.is_open
        from pipeline_type
        inner join
            sfdc_opportunity_snapshot_history_xf history
            on history.opportunity_id = pipeline_type.opportunity_id
            and history.snapshot_date = pipeline_type.max_snapshot_date

    ),
    report_opportunity_pipeline_type as (

        select

            pipe.opportunity_id,
            -- descriptive cuts
            opty.order_type_stamped,
            opty.sales_qualified_source,
            opty.deal_category,
            opty.deal_group,
            opty.opportunity_category,
            opty.sales_team_cro_level,
            opty.sales_team_rd_asm_level,
            -- pipeline fields
            pipe.close_fiscal_quarter_date as report_fiscal_quarter_date,
            pipe.close_fiscal_quarter_name as report_fiscal_quarter_name,
            pipe.close_fiscal_year as report_fiscal_year,
            pipe.pipeline_type,

            case
                when pipe.close_fiscal_quarter_date = opty.close_fiscal_quarter_date
                then 1
                else 0
            end as is_closed_in_quarter_flag,
            case
                when
                    pipe.close_fiscal_quarter_date = opty.close_fiscal_quarter_date
                    and opty.order_type_stamped not in ('4. Contraction')
                    and opty.is_won = 1
                then '1. Closed Won'
                -- the close date for churned deals is updated to the last day before
                -- renewal
                when
                    opty.order_type_stamped
                    in ('5. Churn - Partial', '6. Churn - Final')
                then '6. Churned'
                when opty.order_type_stamped in ('4. Contraction')
                then '5. Contraction'
                when
                    pipe.close_fiscal_quarter_date = opty.close_fiscal_quarter_date
                    and opty.is_lost = 1
                then '4. Closed Lost'
                when
                    pipe.close_fiscal_quarter_date = opty.close_fiscal_quarter_date
                    and opty.is_open = 1
                then '7. Open'
                when
                    pipe.close_fiscal_quarter_date = opty.close_fiscal_quarter_date
                    and opty.stage_name
                    in ('9-Unqualified', '10-Duplicate', 'Unqualified')
                then '8. Duplicate / Unqualified'
                when
                    pipe.close_fiscal_quarter_date <> opty.close_fiscal_quarter_date
                    and pipe.max_snapshot_day_of_fiscal_quarter_normalised >= 75
                then '2. Slipped'
                when
                    pipe.close_fiscal_quarter_date <> opty.close_fiscal_quarter_date
                    and pipe.max_snapshot_day_of_fiscal_quarter_normalised < 75
                then '3. Pushed Out'
                else '9. Other'
            end as pipe_resolution,

            case
                when
                    pipe_resolution
                    in ('1. Closed Won', '4. Closed Lost', '8. Duplicate / Unqualified')
                then pipe.end_close_date
                when
                    pipe_resolution in ('5. Contraction', '6. Churned')
                    and pipe.close_fiscal_quarter_date = opty.close_fiscal_quarter_date
                then pipe.end_close_date
                when
                    pipe_resolution
                    in ('2. Slipped', '3. Pushed Out', '7. Open', '9. Other')
                then pipe.max_snapshot_date
                else null
            end as pipe_resolution_date,

            -- basic net arr
            coalesce(
                pipe.starting_net_arr,
                pipe.pipeline_created_net_arr,
                pipe.pipeline_pull_net_arr,
                0
            ) as quarter_start_net_arr,
            coalesce(
                pipe.starting_stage, pipe.pipeline_created_stage
            ) as quarter_start_stage,
            coalesce(
                pipe.starting_forecast_category, pipe.pipeline_created_forecast_category
            ) as quarter_start_forecast_category,
            coalesce(
                pipe.starting_close_date, pipe.pipeline_created_close_date
            ) as quarter_start_close_date,
            coalesce(
                pipe.starting_is_open, pipe.created_is_open
            ) as quarter_start_is_open,
            coalesce(pipe.starting_is_won, pipe.created_is_won) as quarter_start_is_won,
            coalesce(
                pipe.starting_is_lost, pipe.created_is_lost
            ) as quarter_start_is_lost,

            -- last day the opportunity was closing in quarter
            last_day.net_arr as last_day_net_arr,
            last_day.booked_net_arr as last_day_booked_net_arr,
            last_day.stage_name as last_day_stage_name,
            last_day.forecast_category_name as last_day_forecast_category,
            last_day.is_won as last_day_end_is_won,
            last_day.is_lost as last_day_end_is_lost,
            last_day.is_open as last_day_end_is_open,

            -- last day of the quarter, at this point the deal might not be closing
            -- on the quarter
            pipe.end_booked_net_arr as quarter_end_booked_net_arr,
            pipe.end_net_arr as quarter_end_net_arr,
            pipe.end_stage as quarter_end_stage,
            pipe.end_stage_category as quarter_end_stage_category,
            pipe.end_forecast_category as quarter_end_forecast_category,
            pipe.end_close_date as quarter_end_close_date,
            pipe.end_is_won as quarter_end_is_won,
            pipe.end_is_lost as quarter_end_is_lost,
            pipe.end_is_open as quarter_end_is_open,

            opty.is_renewal as is_renewal,

            last_day.net_arr - quarter_start_net_arr as within_quarter_delta_net_arr,

            -- --------
            -- extra fields for trouble shooting
            pipe.min_snapshot_day_of_fiscal_quarter_normalised,
            pipe.max_snapshot_day_of_fiscal_quarter_normalised,

            pipe.min_snapshot_date,
            pipe.max_snapshot_date,

            pipe.min_close_date,
            pipe.max_close_date,

            pipe.min_net_arr,
            pipe.max_net_arr,

            pipe.min_stage_name,
            pipe.max_stage_name,

            -- --------
            current_date as last_updated_at

        from pipeline_type pipe
        cross join today_date
        inner join sfdc_opportunity_xf opty on opty.opportunity_id = pipe.opportunity_id
        left join
            pipeline_last_day_in_snapshot_quarter last_day
            on last_day.opportunity_id = pipe.opportunity_id
            and pipe.close_fiscal_quarter_date = last_day.close_fiscal_quarter_date
    )

select *
from report_opportunity_pipeline_type
