with
    current_employees as (select * from {{ ref("employee_directory") }}),
    hires as (

        select
            first_name,
            last_name,
            employee_number,
            last_work_email,
            'A' as action,
            hire_date as event_date
        from current_employees
        where hire_date is not null

    ),
    terminations as (

        select
            first_name,
            last_name,
            employee_number,
            last_work_email,
            'D' as action,
            termination_date as event_date
        from current_employees
        where termination_date is not null

    ),
    unioned as (select * from hires UNION ALL select * from terminations),
    report_table as (

        select
            '3936' as "Fund",
            first_name as "First Name",
            last_name as "Last Name",
            employee_number as "Employee ID",
            last_work_email as "Email",
            action as "Action",
            current_date() as report_date
        from unioned
        where
            (
                case
                    when
                        dayofmonth(report_date) <= 15 and dayofmonth(
                            event_date
                        ) > 15 and date_trunc(
                            'month', dateadd('month', -1, report_date)
                        ) = date_trunc('month', event_date)
                    then true
                    when
                        dayofmonth(report_date) > 15 and dayofmonth(
                            event_date
                        ) <= 15 and date_trunc('month', report_date) = date_trunc(
                            'month', event_date
                        )
                    then true
                    else false
                end
            ) = true

    )

select *
from report_table
