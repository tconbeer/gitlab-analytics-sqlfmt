{% set repeated_column_names = "job_id,         requisition_id,         is_prospect,         current_stage_name,         application_status,         job_name,         department_name,         division_modified,         source_name,         source_type,         sourcer_name,         is_outbound,         is_sourced,         candidate_recruiter,         candidate_coordinator,         rejection_reason_name,         rejection_reason_type,         current_job_req_status,         is_hired_in_bamboo,         time_to_offer" %}


with
    stages as (

        select *
        from {{ ref("greenhouse_application_stages_source") }}
        where stage_entered_on is not null

    ),
    stages_pivoted as (

        select
            application_id,
            {{
                dbt_utils.pivot(
                    "stage_name_modified_with_underscores",
                    dbt_utils.get_column_values(
                        ref("greenhouse_application_stages_source"),
                        "stage_name_modified_with_underscores",
                    ),
                    agg="MAX",
                    then_value="stage_entered_on",
                    else_value="NULL",
                    quote_identifiers=TRUE,
                )
            }}
        from {{ ref("greenhouse_application_stages_source") }}
        group by application_id

    ),
    recruiting_xf as (select * from {{ ref("greenhouse_recruiting_xf") }}),
    hires_data as (

        select application_id, candidate_id, hire_date_mod
        from {{ ref("greenhouse_hires") }}

    ),
    applications as (

        select
            application_id,
            candidate_id,
            'Application Submitted' as application_stage,
            true as is_milestone_stage,
            date_trunc(month, application_date) as application_month,
            application_date as stage_entered_on,
            null as stage_exited_on,
            {{ repeated_column_names }}
        from recruiting_xf

    ),
    stages_intermediate as (

        select
            stages.application_id,
            candidate_id,
            stages.stage_name_modified as application_stage,
            stages.is_milestone_stage,
            date_trunc(month, application_date) as application_month,
            iff(
                application_stage_name = 'Offer',
                offer_sent_date,
                stages.stage_entered_on
            ) as stage_entered_on,
            iff(
                application_stage_name = 'Offer',
                offer_resolved_date,
                coalesce(stages.stage_exited_on, current_date())
            ) as stage_exited_on,
            {{ repeated_column_names }}
        from stages
        left join recruiting_xf on recruiting_xf.application_id = stages.application_id

    ),
    hired as (

        select
            hires_data.application_id,
            hires_data.candidate_id,
            'Hired' as application_stage,
            true as is_milestone_stage,
            date_trunc(month, application_date) as application_month,
            hire_date_mod as stage_entered_on,
            hire_date_mod as stage_exited_on,
            {{ repeated_column_names }}
        from hires_data
        left join
            recruiting_xf on recruiting_xf.application_id = hires_data.application_id

    ),
    rejected as (

        select
            application_id,
            candidate_id,
            'Rejected' as application_stage,
            true as is_milestone_stage,
            date_trunc(month, application_date) as application_month,
            rejected_date as stage_entered_on,
            rejected_date as stage_exited_on,
            {{ repeated_column_names }}
        from recruiting_xf
        where application_status in ('rejected')

    ),
    all_stages as (

        select *
        from applications

        union all

        select *
        from stages_intermediate

        union all

        select *
        from hired

        union all

        select *
        from rejected

    ),
    stages_hit as (

        select
            application_id,
            candidate_id,
            min(stage_entered_on) as min_stage_entered_on,
            max(stage_exited_on) as max_stage_exited_on,
            sum(
                iff(application_stage = 'Application Submitted', 1, 0)
            ) as hit_application_submitted,
            sum(
                iff(application_stage = 'Application Review', 1, 0)
            ) as hit_application_review,
            sum(iff(application_stage = 'Assessment', 1, 0)) as hit_assessment,
            sum(iff(application_stage = 'Screen', 1, 0)) as hit_screening,
            sum(
                iff(application_stage = 'Team Interview - Face to Face', 1, 0)
            ) as hit_team_interview,
            sum(
                iff(application_stage = 'Reference Check', 1, 0)
            ) as hit_reference_check,
            sum(iff(application_stage = 'Offer', 1, 0)) as hit_offer,
            sum(iff(application_stage = 'Hired', 1, 0)) as hit_hired,
            sum(iff(application_stage = 'Rejected', 1, 0)) as hit_rejected
        from all_stages
        group by 1, 2

    ),
    intermediate as (

        select
            all_stages.*,
            row_number() over (
                partition by application_id, candidate_id order by stage_entered_on desc
            ) as row_number_stages_desc
        from all_stages

    ),
    stage_order_revamped as (

        select
            intermediate.*,
            case
                when
                    application_stage in ('Hired', 'Rejected') and (
                        hit_rejected = 1 or hit_hired = 1
                    )
                then 1
                when (hit_rejected = 1 or hit_hired = 1)
                then row_number_stages_desc + 1
                else row_number_stages_desc
            end as row_number_stages_desc_updated
        from intermediate
        left join
            stages_hit
            on intermediate.application_id = stages_hit.application_id
            and intermediate.candidate_id = stages_hit.candidate_id

    ),
    final as (

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "stage_order_revamped.application_id",
                        "stage_order_revamped.candidate_id",
                    ]
                )
            }} as unique_key,
            stage_order_revamped.application_id,
            stage_order_revamped.candidate_id,
            application_stage,
            is_milestone_stage,
            stage_entered_on,
            stage_exited_on,
            lead(application_stage) over
            (
                partition by
                    stage_order_revamped.application_id,
                    stage_order_revamped.candidate_id
                order by row_number_stages_desc_updated desc
            ) as next_stage,
            lead(stage_entered_on) over
            (
                partition by
                    stage_order_revamped.application_id,
                    stage_order_revamped.candidate_id
                order by row_number_stages_desc desc
            ) as next_stage_entered_on,
            date_trunc(month, stage_entered_on) as month_stage_entered_on,
            date_trunc(month, stage_exited_on) as month_stage_exited_on,
            datediff(
                day, stage_entered_on, coalesce(stage_exited_on, current_date())
            ) as days_in_stage,
            datediff(
                day, stage_entered_on, coalesce(next_stage_entered_on, current_date())
            ) as days_between_stages,
            datediff(
                day, min_stage_entered_on, max_stage_exited_on
            ) as days_in_pipeline,
            row_number_stages_desc_updated as row_number_stages_desc,
            iff(row_number_stages_desc_updated = 1, true, false) as is_current_stage,

            application_month,
            {{ repeated_column_names }},
            hit_application_review,
            hit_assessment,
            hit_screening,
            hit_team_interview,
            hit_reference_check,
            hit_offer,
            hit_hired,
            hit_rejected,
            iff(
                hit_team_interview = 0
                and hit_rejected = 1
                and rejection_reason_type = 'They rejected us',
                1,
                0
            ) as candidate_dropout,
            case
                when
                    is_current_stage = true and application_stage not in (
                        'Hired', 'Rejected'
                    )
                    and hit_rejected = 0
                    and hit_hired = 0
                    and current_job_req_status = 'open'
                    and application_status = 'active'
                then true
                else false
            end as in_current_pipeline,
            datediff(
                day, stages_pivoted.application_review, stages_pivoted.screen
            ) as turn_time_app_review_to_screen,
            datediff(
                day, stages_pivoted.screen, stages_pivoted.team_interview
            ) as turn_time_screen_to_interview,
            datediff(
                day, stages_pivoted.team_interview, stages_pivoted.offer
            ) as turn_time_interview_to_offer
        from stage_order_revamped
        left join
            stages_hit
            on stage_order_revamped.application_id = stages_hit.application_id
            and stage_order_revamped.candidate_id = stages_hit.candidate_id
        left join
            hires_data
            on stage_order_revamped.application_id = hires_data.application_id
            and stage_order_revamped.candidate_id = hires_data.candidate_id
        left join
            stages_pivoted
            on stages_pivoted.application_id = stage_order_revamped.application_id

    )

select *
from final
