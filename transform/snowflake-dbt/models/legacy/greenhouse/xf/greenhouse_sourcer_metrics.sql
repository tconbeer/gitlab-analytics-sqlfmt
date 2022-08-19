with
    date_details as (

        select date_actual as month_date, 1 as join_field
        from {{ ref("date_details") }}
        where
            date_actual
            between date_trunc(
                month, dateadd(month, -5, current_date())
            ) and current_date()
            and day_of_month = 1

    ),
    recruiting_team as (

        select distinct
            date_trunc(month, date_actual) as month_date, full_name, department
        from {{ ref("employee_directory_analysis") }}
        where
            department like '%Recruiting%'
            and date_actual
            between date_trunc(
                month, dateadd(month, -12, current_date())
            ) and date_trunc(month, current_date())

    ),
    recruiting_data as (

        select *, 1 as join_field
        from {{ ref("greenhouse_stage_analysis") }}
        where sourcer_name is not null and source_type = 'Prospecting'

    ),
    base as (

        select
            date_details.month_date,
            recruiting_data.sourcer_name,
            iff(recruiting_team.full_name is not null, 1, 0) as part_of_recruiting_team
        from date_details
        left join
            recruiting_data on date_details.join_field = recruiting_data.join_field
        left join
            recruiting_team
            on date_details.month_date = recruiting_team.month_date
            and recruiting_data.sourcer_name = recruiting_team.full_name
        group by 1, 2, 3

    ),
    metrics as (

        select
            month_date,
            base.sourcer_name,
            base.part_of_recruiting_team,
            sum(iff(application_stage = 'Application Submitted', 1, 0)) as prospected,
            iff(
                prospected = 0,
                null,
                (
                    sum(
                        iff(
                            application_stage = 'Application Submitted',
                            hit_application_review,
                            0
                        )
                    )
                    / prospected
                )
            ) as prospect_to_review,
            iff(
                prospected = 0,
                null,
                (
                    sum(
                        iff(
                            application_stage = 'Application Submitted',
                            hit_screening,
                            0
                        )
                    )
                    / prospected
                )
            ) as prospect_to_screen,

            iff(
                prospected = 0,
                null,
                sum(iff(application_stage = 'Application Submitted', hit_hired, 0))
                / prospected
            ) as prospect_to_hire,
            iff(
                prospected = 0,
                null,
                sum(
                    iff(
                        application_stage = 'Application Submitted',
                        candidate_dropout,
                        0
                    )
                )
                / prospected
            ) as prospect_to_dropout,

            sum(iff(application_stage = 'Application Review', 1, 0)) as app_reviewed,
            iff(
                app_reviewed = 0,
                null,
                (
                    sum(iff(application_stage = 'Application Review', hit_screening, 0))
                    / app_reviewed
                )
            ) as review_to_screen,
            iff(
                app_reviewed = 0,
                null,
                sum(iff(application_stage = 'Application Review', hit_hired, 0))
                / app_reviewed
            ) as review_to_hire,


            sum(iff(application_stage = 'Screen', 1, 0)) as screen,
            iff(
                screen = 0,
                null,
                sum(iff(application_stage = 'Screen', hit_team_interview, 0)) / screen
            ) as screen_to_interview,
            iff(
                screen = 0,
                null,
                sum(iff(application_stage = 'Screen', hit_hired, 0)) / screen
            ) as screen_to_hire,


            sum(
                iff(application_stage = 'Team Interview - Face to Face', 1, 0)
            ) as team_interview,
            iff(
                team_interview = 0,
                null,
                sum(
                    iff(
                        application_stage = 'Team Interview - Face to Face',
                        hit_hired,
                        0
                    )
                )
                / team_interview
            ) as interview_to_hire,
            iff(
                team_interview = 0,
                null,
                sum(
                    iff(
                        application_stage = 'Team Interview - Face to Face',
                        hit_rejected,
                        0
                    )
                )
                / team_interview
            ) as interview_to_reject,

            sum(
                iff(application_stage = 'Executive Interview', 1, 0)
            ) as executive_interview,
            iff(
                executive_interview = 0,
                null,
                sum(iff(application_stage = 'Executive Interview', hit_hired, 0))
                / executive_interview
            ) as exec_interview_to_hire,

            sum(iff(application_stage = 'Reference Check', 1, 0)) as reference_check,

            sum(
                iff(application_stage = 'Rejected', candidate_dropout, 0)
            ) as candidate_dropout,

            sum(iff(application_stage = 'Offer', 1, 0)) as offer,
            iff(
                offer = 0,
                null,
                sum(iff(application_stage = 'Offer', hit_hired, 0)) / offer
            ) as ofer_to_hire,

            sum(iff(application_stage = 'Hired', 1, 0)) as hired,

            median(
                iff(application_stage = 'Hired', time_to_offer, null)
            ) as time_to_offer_median
        from base
        left join
            recruiting_data
            on base.month_date = recruiting_data.month_stage_entered_on
            and base.sourcer_name = recruiting_data.sourcer_name
        group by 1, 2, 3

    )

select *
from metrics
