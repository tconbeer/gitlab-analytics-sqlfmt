with
    employees as (select * from {{ ref("employee_directory") }}),
    greenhouse_applications as (

        select * from {{ ref("greenhouse_applications_source") }}

    ),
    greenhouse_openings as (select * from {{ ref("greenhouse_openings_source") }}),
    greenhouse_jobs as (select * from {{ ref("greenhouse_jobs_source") }}),
    bamboohr_job_info as (

        select *
        from {{ ref("bamboohr_job_info_source") }}
        qualify row_number() OVER (partition by employee_id order by effective_date) = 1

    ),
    aggregated as (

        select
            opening_id,
            job_name as job_opening_name,
            greenhouse_jobs.job_opened_at,
            concat(first_name, ' ', last_name) as full_name,
            department as department_hired_into,
            division as division_hired_into,
            job_title as job_hired_into
        from employees
        inner join
            greenhouse_applications
            on employees.greenhouse_candidate_id = greenhouse_applications.candidate_id
        inner join
            greenhouse_openings
            on greenhouse_openings.hired_application_id
            = greenhouse_applications.application_id
        inner join
            greenhouse_jobs on greenhouse_jobs.job_id = greenhouse_openings.job_id
        inner join
            bamboohr_job_info on bamboohr_job_info.employee_id = employees.employee_id
        where greenhouse_candidate_id is not null

    )

select *
from aggregated
