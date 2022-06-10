{{ config(tags=["mnpi_exception"]) }}

{{ config(alias="report_opportunity_stages_dates") }}

-- TODO
-- Check out for deals created in a stage that is not 0, use the creation date
with
    sfdc_opportunity_field_history as (

        select * from {{ ref("sfdc_opportunity_field_history_source") }}

    ),
    date_details as (select * from {{ ref("wk_sales_date_details") }}),
    -- after every stage change, as it is a tracked field
    -- a record would be created in the field history table
    sfdc_opportunity_xf as (select * from {{ ref("wk_sales_sfdc_opportunity_xf") }}),
    history_base as (

        select
            opportunity_id,
            replace(
                replace(
                    replace(
                        replace(
                            replace(new_value_string, '2-Developing', '2-Scoping'),
                            '7 - Closing',
                            '7-Closing'
                        ),
                        'Developing',
                        '2-Scoping'
                    ),
                    'Closed Lost',
                    '8-Closed Lost'
                ),
                '8-8-Closed Lost',
                '8-Closed Lost'
            ) as new_value_string,
            min(field_modified_at::date) as min_stage_date

        from sfdc_opportunity_field_history
        where opportunity_field = 'stagename'
        group by 1, 2

    -- just created opportunities won't have any historical record
    -- next CTE accounts for them
    ),
    opty_base as (

        select o.opportunity_id, o.stage_name, o.created_date as min_stage_date
        from sfdc_opportunity_xf o
        left join
            (
                select distinct opportunity_id from history_base
            ) h on h.opportunity_id = o.opportunity_id
        where h.opportunity_id is null

    ),
    combined as (

        select opportunity_id, new_value_string as stage_name, min_stage_date
        from history_base
        union
        select opportunity_id, stage_name, min_stage_date
        from opty_base

    ),
    pivoted_combined as (

        select
            opportunity_id,
            min(
                case
                    when stage_name = '0-Pending Acceptance'
                    then min_stage_date
                    else null
                end
            ) as min_stage_0_date,
            min(
                case when stage_name = '1-Discovery' then min_stage_date else null end
            ) as min_stage_1_date,
            min(
                case when stage_name = '2-Scoping' then min_stage_date else null end
            ) as min_stage_2_date,
            min(
                case
                    when stage_name = '3-Technical Evaluation'
                    then min_stage_date
                    else null
                end
            ) as min_stage_3_date,
            min(
                case when stage_name = '4-Proposal' then min_stage_date else null end
            ) as min_stage_4_date,
            min(
                case when stage_name = '5-Negotiating' then min_stage_date else null end
            ) as min_stage_5_date,
            min(
                case
                    when stage_name = '6-Awaiting Signature'
                    then min_stage_date
                    else null
                end
            ) as min_stage_6_date,
            min(
                case when stage_name = '7-Closing' then min_stage_date else null end
            ) as min_stage_7_date,
            min(
                case when stage_name = '8-Closed Lost' then min_stage_date else null end
            ) as min_stage_8_lost_date,
            min(
                case when stage_name = 'Closed Won' then min_stage_date else null end
            ) as min_stage_8_won_date,
            min(
                case when stage_name = '9-Unqualified' then min_stage_date else null end
            ) as min_stage_9_date,
            min(
                case when stage_name = '10-Duplicate' then min_stage_date else null end
            ) as min_stage_10_date,
            max(
                case
                    when
                        stage_name in (
                            '8-Closed Lost',
                            'Closed Won',
                            '10-Duplicate',
                            '9-Unqualified'
                        )
                    then min_stage_date
                    else null
                end
            ) as max_closed_stage_date,
            max(
                case
                    when
                        stage_name in ('8-Closed Lost', '10-Duplicate', '9-Unqualified')
                    then min_stage_date
                    else null
                end
            ) as max_closed_lost_unqualified_duplicate_date

        from combined
        group by 1

    ),
    pre_final as (

        select
            base.opportunity_id,
            opty.stage_name,
            opty.close_date,
            opty.sales_team_cro_level,
            opty.sales_team_rd_asm_level,
            -- adjusted dates for throughput analysis
            -- missing stage dates are completed using the next available stage date,
            -- up to a closed date
            coalesce(base.min_stage_0_date, opty.created_date) as stage_0_date,
            coalesce(
                base.min_stage_1_date,
                base.min_stage_2_date,
                base.min_stage_3_date,
                base.min_stage_4_date,
                base.min_stage_5_date,
                base.min_stage_6_date,
                base.min_stage_7_date,
                base.min_stage_8_won_date
            ) as stage_1_date,
            coalesce(
                base.min_stage_2_date,
                base.min_stage_3_date,
                base.min_stage_4_date,
                base.min_stage_5_date,
                base.min_stage_6_date,
                base.min_stage_7_date,
                base.min_stage_8_won_date
            ) as stage_2_date,
            coalesce(
                base.min_stage_3_date,
                base.min_stage_4_date,
                base.min_stage_5_date,
                base.min_stage_6_date,
                base.min_stage_7_date,
                base.min_stage_8_won_date
            ) as stage_3_date,
            coalesce(
                base.min_stage_4_date,
                base.min_stage_5_date,
                base.min_stage_6_date,
                base.min_stage_7_date,
                base.min_stage_8_won_date
            ) as stage_4_date,
            coalesce(
                base.min_stage_5_date,
                base.min_stage_6_date,
                base.min_stage_7_date,
                base.min_stage_8_won_date
            ) as stage_5_date,
            coalesce(
                base.min_stage_6_date, base.min_stage_7_date, min_stage_8_won_date
            ) as stage_6_date,
            coalesce(base.min_stage_7_date, base.min_stage_8_won_date) as stage_7_date,
            base.min_stage_8_lost_date as stage_8_lost_date,
            base.min_stage_8_won_date as stage_8_won_date,
            base.min_stage_9_date as stage_9_date,
            base.min_stage_10_date as stage_10_date,
            base.max_closed_stage_date as stage_closed_date,
            base.max_closed_lost_unqualified_duplicate_date
            as stage_close_lost_unqualified_duplicate_date,

            -- unadjusted fields
            base.min_stage_0_date,
            base.min_stage_1_date,
            base.min_stage_2_date,
            base.min_stage_3_date,
            base.min_stage_4_date,
            base.min_stage_5_date,
            base.min_stage_6_date,
            base.min_stage_7_date
        from pivoted_combined base
        inner join sfdc_opportunity_xf opty on opty.opportunity_id = base.opportunity_id
    ),
    final as (

        select
            base.*,

            -- was stage skipped flag
            case
                when base.min_stage_0_date is null and base.stage_0_date is not null
                then 1
                else 0
            end as was_stage_0_skipped_flag,
            case
                when base.min_stage_1_date is null and base.stage_1_date is not null
                then 1
                else 0
            end as was_stage_1_skipped_flag,
            case
                when base.min_stage_2_date is null and base.stage_2_date is not null
                then 1
                else 0
            end as was_stage_2_skipped_flag,
            case
                when base.min_stage_3_date is null and base.stage_3_date is not null
                then 1
                else 0
            end as was_stage_3_skipped_flag,
            case
                when base.min_stage_4_date is null and base.stage_4_date is not null
                then 1
                else 0
            end as was_stage_4_skipped_flag,
            case
                when base.min_stage_5_date is null and base.stage_5_date is not null
                then 1
                else 0
            end as was_stage_5_skipped_flag,
            case
                when base.min_stage_6_date is null and base.stage_6_date is not null
                then 1
                else 0
            end as was_stage_6_skipped_flag,
            case
                when base.min_stage_7_date is null and base.stage_7_date is not null
                then 1
                else 0
            end as was_stage_7_skipped_flag,

            -- calculate age in stage
            datediff(
                day,
                coalesce(
                    base.stage_1_date,
                    base.stage_close_lost_unqualified_duplicate_date,
                    current_date
                ),
                base.stage_0_date
            ) as days_in_stage_0,
            datediff(
                day,
                coalesce(
                    base.stage_2_date,
                    base.stage_close_lost_unqualified_duplicate_date,
                    current_date
                ),
                base.stage_1_date
            ) as days_in_stage_1,
            datediff(
                day,
                coalesce(
                    base.stage_3_date,
                    base.stage_close_lost_unqualified_duplicate_date,
                    current_date
                ),
                base.stage_2_date
            ) as days_in_stage_2,
            datediff(
                day,
                coalesce(
                    base.stage_4_date,
                    base.stage_close_lost_unqualified_duplicate_date,
                    current_date
                ),
                base.stage_3_date
            ) as days_in_stage_3,
            datediff(
                day,
                coalesce(
                    base.stage_5_date,
                    base.stage_close_lost_unqualified_duplicate_date,
                    current_date
                ),
                base.stage_4_date
            ) as days_in_stage_4,
            datediff(
                day,
                coalesce(
                    base.stage_6_date,
                    base.stage_close_lost_unqualified_duplicate_date,
                    current_date
                ),
                base.stage_5_date
            ) as days_in_stage_5,
            datediff(
                day,
                coalesce(
                    base.stage_7_date,
                    base.stage_close_lost_unqualified_duplicate_date,
                    current_date
                ),
                base.stage_6_date
            ) as days_in_stage_6,
            datediff(
                day, coalesce(base.stage_closed_date, current_date), base.stage_7_date
            ) as days_in_stage_7,

            -- stage date helpers
            stage_0.fiscal_quarter_name_fy as stage_0_fiscal_quarter_name,
            stage_0.first_day_of_fiscal_quarter as stage_0_fiscal_quarter_date,
            stage_0.fiscal_year as stage_0_fiscal_year,

            stage_1.fiscal_quarter_name_fy as stage_1_fiscal_quarter_name,
            stage_1.first_day_of_fiscal_quarter as stage_1_fiscal_quarter_date,
            stage_1.fiscal_year as stage_1_fiscal_year,

            stage_2.fiscal_quarter_name_fy as stage_2_fiscal_quarter_name,
            stage_2.first_day_of_fiscal_quarter as stage_2_fiscal_quarter_date,
            stage_2.fiscal_year as stage_2_fiscal_year,

            stage_3.fiscal_quarter_name_fy as stage_3_fiscal_quarter_name,
            stage_3.first_day_of_fiscal_quarter as stage_3_fiscal_quarter_date,
            stage_3.fiscal_year as stage_3_fiscal_year,

            stage_4.fiscal_quarter_name_fy as stage_4_fiscal_quarter_name,
            stage_4.first_day_of_fiscal_quarter as stage_4_fiscal_quarter_date,
            stage_4.fiscal_year as stage_4_fiscal_year,

            stage_5.fiscal_quarter_name_fy as stage_5_fiscal_quarter_name,
            stage_5.first_day_of_fiscal_quarter as stage_5_fiscal_quarter_date,
            stage_5.fiscal_year as stage_5_fiscal_year,

            stage_6.fiscal_quarter_name_fy as stage_6_fiscal_quarter_name,
            stage_6.first_day_of_fiscal_quarter as stage_6_fiscal_quarter_date,
            stage_6.fiscal_year as stage_6_fiscal_year,

            stage_7.fiscal_quarter_name_fy as stage_7_fiscal_quarter_name,
            stage_7.first_day_of_fiscal_quarter as stage_7_fiscal_quarter_date,
            stage_7.fiscal_year as stage_7_fiscal_year

        from pre_final base
        left join date_details stage_0 on stage_0.date_actual = base.stage_0_date::date
        left join date_details stage_1 on stage_1.date_actual = base.stage_1_date::date
        left join date_details stage_2 on stage_2.date_actual = base.stage_2_date::date
        left join date_details stage_3 on stage_3.date_actual = base.stage_3_date::date
        left join date_details stage_4 on stage_4.date_actual = base.stage_4_date::date
        left join date_details stage_5 on stage_5.date_actual = base.stage_5_date::date
        left join date_details stage_6 on stage_6.date_actual = base.stage_6_date::date
        left join date_details stage_7 on stage_7.date_actual = base.stage_7_date::date

    )

select *
from final
