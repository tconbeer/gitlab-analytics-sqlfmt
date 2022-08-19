with
    source as (

        select
            month_year as month_date,
            case
                when department = 'People' and month_year <= '2019-08-31'
                then 'People Ops'
                when department = 'People' and month_year >= '2020-05-31'
                then 'People Success'
                when department = 'Brand and Digital Design'
                then 'Brand & Digital Design'
                when department = 'Outreach' and month_year < '2020-02-29'
                then 'Community Relations'
                else department
            end as department,
            plan as headcount
        from {{ ref("sheetload_hire_plan") }}
        where month_year <= '2020-05-31'

    ),
    employee_directory as (select * from {{ ref("employee_directory_analysis") }}),
    department_division_mapping as (

        select distinct
            department, department_modified, division_mapped_current as division
        from {{ ref("bamboohr_job_info_current_division_base") }}
        where department is not null

    ),
    all_company as (

        select
            source.month_date,
            'all_company_breakout' as breakout_type,
            'all_company_breakout' as department,
            'all_company_breakout' as division,
            sum(headcount) as planned_headcount,
            planned_headcount
            - lag(planned_headcount) over (order by month_date) as planned_hires
        from source
        group by 1, 2, 3

    ),
    division_level as (

        select
            source.month_date,
            'division_breakout' as breakout_type,
            'division_breakout' as department,
            department_division_mapping.division,
            sum(headcount) as planned_headcount,
            planned_headcount
            - lag(planned_headcount) over (
                partition by department_division_mapping.division
                order by source.month_date
            ) as planned_hires
        from source
        left join
            department_division_mapping
            on department_division_mapping.department = source.department
        group by 1, 2, 3, 4

    ),
    department_level as (

        select
            source.month_date,
            'department_division_breakout' as breakout_type,
            source.department as department,
            department_division_mapping.division,
            sum(headcount) as planned_headcount,
            planned_headcount
            - lag(planned_headcount) over (
                partition by department_division_mapping.division, source.department
                order by source.month_date
            ) as planned_hires
        from source
        left join
            department_division_mapping
            on department_division_mapping.department = source.department
        group by 1, 2, 3, 4

    )

select *
from all_company

union all

select *
from division_level

union all

select *
from department_level
