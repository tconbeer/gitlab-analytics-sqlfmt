{{
    simple_cte(
        [
            ("job_openings", "greenhouse_openings_source"),
            ("jobs", "greenhouse_jobs_source"),
            ("application_jobs", "greenhouse_applications_jobs_source"),
            ("applications", "greenhouse_applications_source"),
            ("sources", "greenhouse_sources_source"),
            ("departments", "wk_prep_greenhouse_departments"),
        ]
    )
}},
job_departments as (

    select *
    from {{ ref("greenhouse_jobs_departments_source") }}
    -- Table is many to many (job_id to department_id) with the lowest level created
    -- first
    qualify
        row_number() over (partition by job_id order by job_department_created_at asc)
        = 1

),
application_stages as (

    select *
    from {{ ref("greenhouse_application_stages_source") }}
    -- Table can contain duplicate records
    qualify
        row_number() over (
            partition by application_id, stage_id, stage_entered_on
            order by stage_entered_on
        )
        = 1

)


select
    job_openings.job_id,
    job_openings.job_opening_id,
    application_stages.stage_entered_on as stage_entered_date,
    application_stages.stage_exited_on as stage_exited_date,
    application_stages.application_stage_name,
    applications.application_id,
    sources.source_name,
    sources.source_type,
    departments.department_name as greenhouse_department_name,
    departments.level_1 as greenhouse_department_level_1,
    departments.level_2 as greenhouse_department_level_2,
    departments.level_3 as greenhouse_department_level_3
from job_openings
left join jobs on job_openings.job_id = jobs.job_id
left join application_jobs on job_openings.job_id = application_jobs.job_id
left join applications on application_jobs.application_id = applications.application_id
left join
    application_stages
    on application_jobs.application_id = application_stages.application_id
left join sources on applications.source_id = sources.source_id
left join job_departments on job_openings.job_id = job_departments.job_id
left join departments on job_departments.department_id = departments.department_id
where application_stages.stage_entered_on is not null and jobs.job_opened_at is not null
