{% set lines_to_repeat = "DATE_TRUNC(month,hire_date_mod)                                                           AS hire_month,           SUM(IFF(job_opening_type = 'New Hire' AND hire_type != 'Transfer',1,0))                    AS new_hire,           SUM(IFF(job_opening_type = 'New Hire' AND hire_type = 'Transfer',1,0))                     AS new_position_filled_internally,           SUM(IFF(job_opening_type IN                      ('Current Team Member','Internal Transfer'),1,0))                                AS transfers,           SUM(IFF(job_opening_type IN ('Backfill'),1,0))                                             AS backfill,           SUM(IFF(job_opening_type IS NULL,1,0))                                                     AS unidentified_job_opening_type,           COUNT(*)                                                                                   AS total_greenhouse_reqs_filled         FROM greenhouse_hire_type         WHERE hired_in_bamboohr= TRUE         GROUP BY 1,2,3,4" %}

with
    dim_date as (

        select distinct fiscal_year, last_day_of_month as month_date
        from {{ ref("dim_date") }}

    ),
    headcount as (

        select
            month_date,
            case
                when breakout_type = 'kpi_breakout'
                then 'all_company_breakout'
                when breakout_type = 'department_breakout'
                then 'department_division_breakout'
                else breakout_type
            end as breakout_type,
            iff(
                breakout_type = 'kpi_breakout', 'all_company_breakout', department
            ) as department,
            iff(
                breakout_type = 'kpi_breakout', 'all_company_breakout', division
            ) as division,
            coalesce(headcount_end, 0) as headcount_actual,
            coalesce(hire_count, 0) as hires_actual
        from {{ ref("bamboohr_rpt_headcount_aggregation") }}
        where
            breakout_type in (
                'kpi_breakout', 'department_breakout', 'division_breakout'
            ) and eeoc_field_name = 'no_eeoc'

    ),
    hire_plan as (

        select
            *,
            iff(
                date_trunc(month, month_date) = date_trunc(
                    month, dateadd(month, -1, current_date())
                ),
                1,
                0
            ) as last_month
        from {{ ref("hire_replan_xf") }}

    ),
    department_name_changes as (

        select
            trim(old_department_name) as old_department_name,
            trim(new_department_name) as new_department_name,
            change_effective_date
        from {{ ref("department_name_changes") }}

    ),
    greenhouse_hire_type as (select * from {{ ref("greenhouse_hires") }}),
    hire_type_aggregated as (

        select
            'department_division_breakout' as breakout_type,
            division,
            department,
            {{ lines_to_repeat }}

        UNION ALL

        select
            'division_breakout' as breakout_type,
            division,
            'division_breakout' as department,
            {{ lines_to_repeat }}

        UNION ALL

        select
            'all_company_breakout' as breakout_type,
            'all_company_breakout' as division,
            'all_company_breakout' as department,
            {{ lines_to_repeat }}

    ),
    joined as (

        select
            dim_date.fiscal_year,
            hire_plan.month_date,
            hire_plan.breakout_type,
            coalesce(
                trim(department_name_changes.new_department_name), hire_plan.department
            ) as department,
            hire_plan.division,
            hire_plan.planned_headcount,
            hire_plan.planned_hires,
            coalesce(headcount.headcount_actual, 0) as headcount_actual,
            coalesce(headcount.hires_actual, 0) as hires_actual,
            iff(
                hire_plan.planned_headcount = 0,
                null,
                round( (headcount.headcount_actual / hire_plan.planned_headcount), 4)
            ) as actual_headcount_vs_planned_headcount,

            new_hire,
            transfers,
            backfill,
            unidentified_job_opening_type,
            total_greenhouse_reqs_filled,
            new_hire + backfill as total_hires_greenhouse
        from dim_date
        left join hire_plan on dim_date.month_date = hire_plan.month_date
        left join
            department_name_changes
            on department_name_changes.old_department_name = hire_plan.department
        left join
            headcount
            on headcount.breakout_type = hire_plan.breakout_type
            and headcount.department = coalesce(
                department_name_changes.new_department_name, hire_plan.department
            )
            and headcount.division = hire_plan.division
            and headcount.month_date = date_trunc(month, hire_plan.month_date)
        left join
            hire_type_aggregated
            on hire_type_aggregated.breakout_type = hire_plan.breakout_type
            and hire_type_aggregated.department = coalesce(
                department_name_changes.new_department_name, hire_plan.department
            )
            and hire_type_aggregated.division = hire_plan.division
            and hire_type_aggregated.hire_month = date_trunc(
                month, hire_plan.month_date
            )

    ),
    final as (

        select
            *,
            sum(planned_hires) over
            (
                partition by fiscal_year, breakout_type, division, department
                order by month_date
                rows between unbounded preceding and current row
            ) as cumulative_planned_hires,
            sum(hires_actual) over
            (
                partition by fiscal_year, breakout_type, division, department
                order by month_date
                rows between unbounded preceding and current row
            ) as cumulative_hires_actual,
            iff(
                cumulative_planned_hires = 0,
                null,
                round( (cumulative_hires_actual / cumulative_planned_hires), 2)
            ) as cumulative_hires_vs_plan
        from joined
        where
            month_date between dateadd(month, -24, current_date()) and dateadd(
                month, 12, current_date()
            )
    )

select *
from final
