with
    hire_replan as (select * from {{ ref("sheetload_hire_replan") }}),
    original_fy21_plan as (select * from {{ ref("hire_plan_xf") }}),
    employee_directory as (

        select
            date_actual,
            iff(
                date_actual > '2020-06-30' and department = 'Alliances',
                'Business Development',
                department
            ) as department,
            -- sheetload file does not have department change reflected
            division,
            employment_status
        from {{ ref("employee_directory_intermediate") }}

    ),
    unpivoted as (

        select department, month::date as month_date, headcount
        from
            hire_replan
            unpivot(
                headcount for month in (
                    "2020-04-30",
                    "2020-05-31",
                    "2020-06-30",
                    "2020-07-31",
                    "2020-08-31",
                    "2020-09-30",
                    "2020-10-31",
                    "2020-11-30",
                    "2020-12-31",
                    "2021-01-31",
                    "2021-02-28",
                    "2021-03-31",
                    "2021-04-30",
                    "2021-05-31",
                    "2021-06-30",
                    "2021-07-31",
                    "2021-08-31",
                    "2021-09-30",
                    "2021-10-31",
                    "2021-11-30",
                    "2021-12-31",
                    "2022-01-31"
                )
            )

    ),
    department_division_mapping as (

        select distinct
            department, department_modified, division_mapped_current as division
        from {{ ref("bamboohr_job_info_current_division_base") }}
        where department is not null

    ),
    department_name_changes as (select * from {{ ref("department_name_changes") }}),
    all_company as (

        select
            unpivoted.month_date,
            'all_company_breakout' as breakout_type,
            'all_company_breakout' as department,
            'all_company_breakout' as division,
            sum(headcount) as planned_headcount,
            planned_headcount
            - lag(planned_headcount) over (order by month_date) as planned_hires
        from unpivoted
        group by 1, 2, 3

    ),
    division_level as (

        select
            unpivoted.month_date,
            'division_breakout' as breakout_type,
            'division_breakout' as department,
            department_division_mapping.division,
            sum(headcount) as planned_headcount,
            planned_headcount
            - lag(planned_headcount) over (
                partition by department_division_mapping.division
                order by unpivoted.month_date
            ) as planned_hires
        from unpivoted
        left join
            department_division_mapping
            on department_division_mapping.department = unpivoted.department
        group by 1, 2, 3, 4

    ),
    department_level as (

        select
            unpivoted.month_date,
            'department_division_breakout' as breakout_type,
            unpivoted.department,
            department_division_mapping.division,
            sum(headcount) as planned_headcount,
            planned_headcount
            - lag(planned_headcount) over (
                partition by department_division_mapping.division, unpivoted.department
                order by unpivoted.month_date
            ) as planned_hires
        from unpivoted
        left join
            department_division_mapping
            on department_division_mapping.department = unpivoted.department
        group by 1, 2, 3, 4

    ),
    unioned as (

        select *
        from original_fy21_plan
        -- --this plan captures the monthly plan prior to 2020.05, whereas the replan
        -- captures months post 2020.05
        union all

        select *
        from all_company
        where month_date >= '2020-06-01'

        union all

        select *
        from division_level
        where month_date >= '2020-06-01'


        union all

        select *
        from department_level
        where month_date >= '2020-06-01'

    )

select
    month_date,
    breakout_type,
    department,
    division,
    planned_headcount,
    iff(planned_hires < 0, 0, coalesce(planned_hires, 0)) as planned_hires
from unioned
