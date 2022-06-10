with
    recruiting_data as (select * from {{ ref("greenhouse_stage_analysis") }}),
    isat as (

        select submitted_at, avg(isat_score) as isat
        from {{ ref("rpt_interviewee_satisfaction_isat") }}
        group by 1

    ),
    metrics as (

        select
            month_stage_entered_on as month_date,
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
                    ) / prospected
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
                    ) / prospected
                )
            ) as prospect_to_screen,
            iff(
                prospected = 0,
                null,
                sum(
                    iff(application_stage = 'Application Submitted', hit_hired, 0)
                ) / prospected
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
                ) / prospected
            ) as prospect_to_dropout,

            sum(iff(application_stage = 'Application Review', 1, 0)) as app_reviewed,
            iff(
                app_reviewed = 0,
                null,
                (
                    sum(
                        iff(application_stage = 'Application Review', hit_screening, 0)
                    ) / app_reviewed
                )
            ) as review_to_screen,
            iff(
                app_reviewed = 0,
                null,
                sum(
                    iff(application_stage = 'Application Review', hit_hired, 0)
                ) / app_reviewed
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
                ) / team_interview
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
                ) / team_interview
            ) as interview_to_reject,

            sum(
                iff(application_stage = 'Executive Interview', 1, 0)
            ) as executive_interview,
            iff(
                executive_interview = 0,
                null,
                sum(
                    iff(application_stage = 'Executive Interview', hit_hired, 0)
                ) / executive_interview
            ) as exec_interview_to_hire,

            sum(iff(application_stage = 'Reference Check', 1, 0)) as reference_check,

            sum(
                iff(application_stage = 'Rejected', candidate_dropout, 0)
            ) as candidate_dropout,

            sum(iff(application_stage = 'Offer', 1, 0)) as offer,
            iff(
                offer = 0,
                null,
                sum(
                    iff(
                        application_stage = 'Offer' and application_status = 'hired',
                        hit_hired,
                        0
                    )
                ) / offer
            ) as offer_acceptance_rate,

            sum(iff(application_stage = 'Hired', 1, 0)) as hired,
            sum(
                iff(
                    application_stage = 'Hired' and source_name != 'Internal Applicant',
                    1,
                    0
                )
            ) as hires_excluding_transfers,

            -- -note hired includes interal applicants whereas
            -- hires_excluding_transfers
            median(
                iff(application_stage = 'Hired', time_to_offer, null)
            ) as time_to_offer_median,
            sum(
                iff(application_stage = 'Hired' and is_sourced = 1, 1, 0)
            ) as sourced_candidate,

            iff(
                hires_excluding_transfers = 0,
                0,
                sourced_candidate / hires_excluding_transfers
            ) as percent_sourced_hires,
            sum(
                iff(application_stage = 'Hired' and is_outbound = 1, 1, 0)
            ) as outbound_candidate,
            iff(
                hires_excluding_transfers = 0,
                0,
                outbound_candidate / hires_excluding_transfers
            ) as percent_outbound_hires

        from recruiting_data
        where unique_key not in ('6d31c2d36d2eaec7f5b36605ac3ccf77')
        group by 1

    ),
    final as (

        select metrics.*, isat.isat
        from metrics
        left join isat on isat.submitted_at = metrics.month_date
        where
            month_date between date_trunc(
                month, dateadd(month, -13, current_date())
            ) and date_trunc(month, current_date())

    )

select *
from final
