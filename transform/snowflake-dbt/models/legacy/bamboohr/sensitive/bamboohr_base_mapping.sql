with
    dates as (

        select *, 'join' as join_field
        from {{ ref("dim_date") }}
        where
            date_actual between dateadd(
                year, -1, dateadd(month, -1, date_trunc(month, current_date()))
            ) and date_trunc(month, current_date())
            and day_of_month = 1

    ),
    division_department_mapping as (

        select *, 'join' as join_field
        from {{ ref("bamboohr_job_info_current_division_base") }}
        where
            date_trunc(month, current_date()) between effective_date and coalesce(
                effective_end_date, termination_date, current_date()
            )

    ),
    unioned as (

        select distinct
            dates.date_actual,
            'division_grouping_breakout' as field_name,
            division_grouping as field_value
        from dates
        left join
            division_department_mapping
            on dates.join_field = division_department_mapping.join_field

        union all

        select distinct
            dates.date_actual,
            'department_grouping_breakout' as field_name,
            department_grouping as field_value
        from dates
        left join
            division_department_mapping
            on dates.join_field = division_department_mapping.join_field

        union all

        select
            dates.date_actual,
            'company_breakout' as field_name,
            'company_breakout' as field_value
        from dates

    )

select *
from unioned
