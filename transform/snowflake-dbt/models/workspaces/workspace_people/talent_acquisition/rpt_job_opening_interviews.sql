

{{
    simple_cte(
        [
            ("job_openings", "rpt_greenhouse_current_openings"),
            ("application_jobs", "greenhouse_applications_jobs_source"),
            ("applications", "greenhouse_applications_source"),
            ("sources", "greenhouse_sources_source"),
            ("interviews", "greenhouse_scheduled_interviews_source"),
            ("departments", "wk_prep_greenhouse_departments"),
        ]
    )
}}

,
job_departments as (

    select *
    from {{ ref("greenhouse_jobs_departments_source") }}
    -- Table is many to many (job_id to department_id) with the lowest level created
    -- first
    qualify
        row_number() over (
            partition by job_id order by job_department_created_at asc
        ) = 1

)

select
    job_openings.job_id,
    job_openings.job_opening_id,
    interviews.application_id,
    interviews.scheduled_interview_id,
    interviews.interview_starts_at,
    interviews.scheduled_interview_stage_name,
    sources.source_name,
    sources.source_type,
    departments.department_name as greenhouse_department_name,
    departments.level_1 as greenhouse_department_level_1,
    departments.level_2 as greenhouse_department_level_2,
    departments.level_3 as greenhouse_department_level_3
from job_openings
left join application_jobs on job_openings.job_id = application_jobs.job_id
left join applications on application_jobs.application_id = applications.application_id
left join sources on applications.source_id = sources.source_id
inner join interviews on applications.application_id = interviews.application_id
left join job_departments on job_openings.job_id = job_departments.job_id
left join departments on job_departments.department_id = departments.department_id
