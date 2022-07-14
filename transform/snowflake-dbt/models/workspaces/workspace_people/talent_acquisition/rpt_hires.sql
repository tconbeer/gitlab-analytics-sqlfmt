{{
    simple_cte(
        [
            ("bamboohr_mapping", "bamboohr_id_employee_number_mapping"),
            ("applications", "greenhouse_applications_source"),
            ("sources", "greenhouse_sources_source"),
            ("offers", "greenhouse_offers_source"),
            ("openings", "greenhouse_openings_source"),
            ("departments", "wk_prep_greenhouse_departments"),
            ("application_jobs", "greenhouse_applications_jobs_source"),
            ("jobs", "greenhouse_jobs_source"),
            ("candidates", "greenhouse_candidates_source"),
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

)

select
    applications.candidate_id,
    applications.application_id,
    bamboohr_mapping.region,
    jobs.job_id,
    jobs.job_name,
    candidates.candidate_recruiter,
    sources.source_name,
    sources.source_type,
    applications.applied_at,
    openings.job_opened_at as job_opening_opened_at,
    openings.job_closed_at as job_opening_closed_at,
    openings.target_start_date as job_opening_target_start_date,
    offers.sent_at as offer_sent_at,
    offers.resolved_at as offer_accepted_at,
    departments.department_name as greenhouse_department_name,
    departments.level_1 as greenhouse_department_level_1,
    departments.level_2 as greenhouse_department_level_2,
    departments.level_3 as greenhouse_department_level_3
from offers
left join applications on offers.application_id = applications.application_id
left join sources on applications.source_id = sources.source_id
left join openings on offers.application_id = openings.hired_application_id
left join
    bamboohr_mapping
    on applications.candidate_id = bamboohr_mapping.greenhouse_candidate_id
left join application_jobs on offers.application_id = application_jobs.application_id
left join job_departments on application_jobs.job_id = job_departments.job_id
left join departments on job_departments.department_id = departments.department_id
left join jobs on application_jobs.job_id = jobs.job_id
left join candidates on applications.candidate_id = candidates.candidate_id
where offers.offer_status = 'accepted'
