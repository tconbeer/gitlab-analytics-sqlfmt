with
    sourcer_metrics as (

        select
            month_date,
            sourcer_name,
            prospected,
            prospect_to_review,
            prospect_to_screen,
            app_reviewed,
            review_to_screen,
            screen,
            screen_to_interview,
            screen_to_hire,
            candidate_dropout
        from {{ ref("greenhouse_sourcer_metrics") }} sourcer_metrics
        where part_of_recruiting_team = 1

    ),
    time_period as (

        select distinct
            date_actual as reporting_month,
            dateadd(month, -3, date_actual) as start_period,
            dateadd(month, -1, date_actual) as end_period
        from {{ ref("date_details") }}
        where
            day_of_month = 1
            and date_actual
            between date_trunc(
                month, dateadd(month, -15, current_date())
            ) and date_trunc(month, current_date())

    ),
    three_month_rolling as (

        select
            time_period.reporting_month,
            time_period.start_period,
            time_period.end_period,
            sourcer_metrics.*
        from time_period
        left join
            sourcer_metrics
            on sourcer_metrics.month_date
            between time_period.start_period and time_period.end_period

    )

select *
from three_month_rolling
