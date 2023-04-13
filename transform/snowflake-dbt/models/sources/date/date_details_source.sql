with
    date_spine as (

        {{
            dbt_utils.date_spine(
                start_date="to_date('11/01/2009', 'mm/dd/yyyy')",
                datepart="day",
                end_date="dateadd(year, 40, current_date)",
            )
        }}

    ),
    calculated as (

        select
            date_day,
            date_day as date_actual,

            dayname(date_day) as day_name,

            date_part('month', date_day) as month_actual,
            date_part('year', date_day) as year_actual,
            date_part(quarter, date_day) as quarter_actual,

            date_part(dayofweek, date_day) + 1 as day_of_week,
            case
                when day_name = 'Sun'
                then date_day
                else dateadd('day', -1, date_trunc('week', date_day))
            end as first_day_of_week,

            case
                when day_name = 'Sun' then week(date_day) + 1 else week(date_day)
            end as week_of_year_temp,  -- remove this column

            case
                when
                    day_name = 'Sun'
                    and lead(week_of_year_temp) over (order by date_day) = '1'
                then '1'
                else week_of_year_temp
            end as week_of_year,

            date_part('day', date_day) as day_of_month,

            row_number() over (
                partition by year_actual, quarter_actual order by date_day
            ) as day_of_quarter,
            row_number() over (
                partition by year_actual order by date_day
            ) as day_of_year,

            case
                when month_actual < 2 then year_actual else (year_actual + 1)
            end as fiscal_year,
            case
                when month_actual < 2
                then '4'
                when month_actual < 5
                then '1'
                when month_actual < 8
                then '2'
                when month_actual < 11
                then '3'
                else '4'
            end as fiscal_quarter,

            row_number() over (
                partition by fiscal_year, fiscal_quarter order by date_day
            ) as day_of_fiscal_quarter,
            row_number() over (
                partition by fiscal_year order by date_day
            ) as day_of_fiscal_year,

            to_char(date_day, 'MMMM') as month_name,

            trunc(date_day, 'Month') as first_day_of_month,
            last_value(date_day) over (
                partition by year_actual, month_actual order by date_day
            ) as last_day_of_month,

            first_value(date_day) over (
                partition by year_actual order by date_day
            ) as first_day_of_year,
            last_value(date_day) over (
                partition by year_actual order by date_day
            ) as last_day_of_year,

            first_value(date_day) over (
                partition by year_actual, quarter_actual order by date_day
            ) as first_day_of_quarter,
            last_value(date_day) over (
                partition by year_actual, quarter_actual order by date_day
            ) as last_day_of_quarter,

            first_value(date_day) over (
                partition by fiscal_year, fiscal_quarter order by date_day
            ) as first_day_of_fiscal_quarter,
            last_value(date_day) over (
                partition by fiscal_year, fiscal_quarter order by date_day
            ) as last_day_of_fiscal_quarter,

            first_value(date_day) over (
                partition by fiscal_year order by date_day
            ) as first_day_of_fiscal_year,
            last_value(date_day) over (
                partition by fiscal_year order by date_day
            ) as last_day_of_fiscal_year,

            datediff('week', first_day_of_fiscal_year, date_actual)
            + 1 as week_of_fiscal_year,

            case
                when extract('month', date_day) = 1
                then 12
                else extract('month', date_day) - 1
            end as month_of_fiscal_year,

            last_value(date_day) over (
                partition by first_day_of_week order by date_day
            ) as last_day_of_week,

            (year_actual || '-Q' || extract(quarter from date_day)) as quarter_name,

            (
                fiscal_year
                || '-'
                || decode(fiscal_quarter, 1, 'Q1', 2, 'Q2', 3, 'Q3', 4, 'Q4')
            ) as fiscal_quarter_name,
            ('FY' || substr(fiscal_quarter_name, 3, 7)) as fiscal_quarter_name_fy,
            dense_rank() over (
                order by fiscal_quarter_name
            ) as fiscal_quarter_number_absolute,
            fiscal_year || '-' || monthname(date_day) as fiscal_month_name,
            ('FY' || substr(fiscal_month_name, 3, 8)) as fiscal_month_name_fy,

            (
                case
                    when month(date_day) = 1 and dayofmonth(date_day) = 1
                    then 'New Year''s Day'
                    when month(date_day) = 12 and dayofmonth(date_day) = 25
                    then 'Christmas Day'
                    when month(date_day) = 12 and dayofmonth(date_day) = 26
                    then 'Boxing Day'
                    else null
                end
            )::varchar as holiday_desc,
            (case when holiday_desc is null then 0 else 1 end)::boolean as is_holiday,
            date_trunc(
                'month', last_day_of_fiscal_quarter
            ) as last_month_of_fiscal_quarter,
            iff(
                date_trunc('month', last_day_of_fiscal_quarter) = date_actual,
                true,
                false
            ) as is_first_day_of_last_month_of_fiscal_quarter,
            date_trunc('month', last_day_of_fiscal_year) as last_month_of_fiscal_year,
            iff(
                date_trunc('month', last_day_of_fiscal_year) = date_actual, true, false
            ) as is_first_day_of_last_month_of_fiscal_year,
            dateadd(
                'day', 7, dateadd('month', 1, first_day_of_month)
            ) as snapshot_date_fpa,
            dateadd(
                'day', 44, dateadd('month', 1, first_day_of_month)
            ) as snapshot_date_billings,
            count(date_actual) over (
                partition by first_day_of_month
            ) as days_in_month_count,
            90 - datediff(
                day, date_actual, last_day_of_fiscal_quarter
            ) as day_of_fiscal_quarter_normalised,
            12 - floor(
                (datediff(day, date_actual, last_day_of_fiscal_quarter) / 7)
            ) as week_of_fiscal_quarter_normalised,
            case
                when week_of_fiscal_quarter_normalised < 5
                then week_of_fiscal_quarter_normalised
                when week_of_fiscal_quarter_normalised < 9
                then week_of_fiscal_quarter_normalised - 4
                else week_of_fiscal_quarter_normalised - 8
            end as week_of_month_normalised,
            365 - datediff(
                day, date_actual, last_day_of_fiscal_year
            ) as day_of_fiscal_year_normalised,
            case
                when
                    (
                        (datediff(day, date_actual, last_day_of_fiscal_quarter) - 6) % 7
                        = 0
                        or date_actual = first_day_of_fiscal_quarter
                    )
                then 1
                else 0
            end as is_first_day_of_fiscal_quarter_week
        from date_spine

    )

select
    date_day,
    date_actual,
    day_name,
    month_actual,
    year_actual,
    quarter_actual,
    day_of_week,
    first_day_of_week,
    week_of_year,
    day_of_month,
    day_of_quarter,
    day_of_year,
    fiscal_year,
    fiscal_quarter,
    day_of_fiscal_quarter,
    day_of_fiscal_year,
    month_name,
    first_day_of_month,
    last_day_of_month,
    first_day_of_year,
    last_day_of_year,
    first_day_of_quarter,
    last_day_of_quarter,
    first_day_of_fiscal_quarter,
    last_day_of_fiscal_quarter,
    first_day_of_fiscal_year,
    last_day_of_fiscal_year,
    week_of_fiscal_year,
    month_of_fiscal_year,
    last_day_of_week,
    quarter_name,
    fiscal_quarter_name,
    fiscal_quarter_name_fy,
    fiscal_quarter_number_absolute,
    fiscal_month_name,
    fiscal_month_name_fy,
    holiday_desc,
    is_holiday,
    last_month_of_fiscal_quarter,
    is_first_day_of_last_month_of_fiscal_quarter,
    last_month_of_fiscal_year,
    is_first_day_of_last_month_of_fiscal_year,
    snapshot_date_fpa,
    snapshot_date_billings,
    days_in_month_count,
    week_of_month_normalised,
    day_of_fiscal_quarter_normalised,
    week_of_fiscal_quarter_normalised,
    day_of_fiscal_year_normalised,
    is_first_day_of_fiscal_quarter_week
from calculated
