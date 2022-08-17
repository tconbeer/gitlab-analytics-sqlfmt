with
    applications as (

        select *
        from {{ ref("greenhouse_applications_source") }}
        where applied_at >= '2017-01-01'

    ),
    offers as (select * from {{ ref("greenhouse_offers_source") }}),
    greenhouse_application_jobs as (

        select * from {{ ref("greenhouse_applications_jobs_source") }}

    ),
    jobs as (select * from {{ ref("greenhouse_jobs_source") }}),
    job_req as (

        select
            applications.application_id,
            jobs.*,
            count(jobs.job_id) over (
                partition by applications.application_id
            ) as total_reqs_for_job_id,
            case
                when total_reqs_for_job_id = 1
                then 1
                when
                    total_reqs_for_job_id > 1
                    and applications.applied_at
                    between jobs.job_created_at and coalesce(
                        jobs.job_closed_at, dateadd(week, 3, current_date())
                    )
                then 1
                else 0
            end as job_req_to_use
        from applications
        left join
            greenhouse_application_jobs
            on applications.application_id = greenhouse_application_jobs.application_id
        left join jobs on greenhouse_application_jobs.job_id = jobs.job_id

    ),
    greenhouse_departments as (

        select * from {{ ref("greenhouse_departments_source") }}

    ),
    greenhouse_sources as (select * from {{ ref("greenhouse_sources_source") }}),
    greenhouse_sourcer as (select * from {{ ref("greenhouse_sourcer") }}),
    candidates as (select * from {{ ref("greenhouse_candidates_source") }}),
    rejection_reasons as (

        select * from {{ ref("greenhouse_rejection_reasons_source") }}

    ),
    cost_center as (

        select distinct division, department
        from {{ ref("cost_center_division_department_mapping_current") }}

    ),
    bamboo as (

        select greenhouse_candidate_id, hire_date
        from {{ ref("bamboohr_id_employee_number_mapping") }}
        where greenhouse_candidate_id is not null

    ),
    final as (

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "applications.application_id",
                        "offers.offer_id",
                        "applications.candidate_id",
                        "job_req.job_id",
                        "job_req.requisition_id",
                    ]
                )
            }} as unique_key,
            applications.application_id,
            offers.offer_id,
            applications.candidate_id,
            job_req.job_id,
            job_req.requisition_id,
            applications.prospect as is_prospect,
            applications.application_status,
            applications.stage_name as current_stage_name,
            offers.offer_status,
            applications.applied_at as application_date,
            offers.sent_at as offer_sent_date,
            offers.resolved_at as offer_resolved_date,
            offers.start_date as candidate_target_hire_date,
            applications.rejected_at as rejected_date,
            job_req.job_name,
            greenhouse_departments.department_name as department_name,
            cost_center.division as division,
            case
                when lower(greenhouse_departments.department_name) like '%sales%'
                then 'Sales'
                when greenhouse_departments.department_name = 'Dev'
                then 'Engineering'
                when
                    greenhouse_departments.department_name
                    = 'Customer Success Management'
                then 'Sales'
                else
                    coalesce(
                        cost_center.division, greenhouse_departments.department_name
                    )
            end as division_modified,
            greenhouse_sources.source_name as source_name,
            greenhouse_sources.source_type as source_type,
            case
                when
                    trim(source_name) in (
                        'Sales Bootcamp',
                        'Social media presence',
                        'Greenhouse',
                        'Maildrop',
                        'Reddit',
                        'Slack Groups',
                        'AmazingHiring',
                        'AngelList',
                        'Google',
                        'Greenhouse Sourcing',
                        'LinkedIn (Prospecting)',
                        'SocialReferral',
                        'Talent Community',
                        'Viren - LinkedIn',
                        'Referral',
                        'LinkedIn (Social Media)',
                        'Twitter'
                    )
                then 1
                when trim(source_type) = 'In person event'
                then 1
                else 0
            end as is_outbound,
            case
                when
                    trim(source_name) in (
                        'Greenhouse',
                        'Maildrop',
                        'Reddit',
                        'Slack Groups',
                        'AmazingHiring',
                        'Google',
                        'Greenhouse Sourcing',
                        'LinkedIn (Prospecting)',
                        'Talent Community',
                        'Viren - LinkedIn',
                        'LinkedIn (Social Media)',
                        'Twitter'
                    )
                then 1
                when trim(source_type) = 'In person event'
                then 1
                else 0
            end as is_sourced,
            greenhouse_sourcer.sourcer_name,
            candidates.candidate_recruiter,
            candidates.candidate_coordinator,
            iff(
                greenhouse_sources.source_name = 'LinkedIn (Prospecting)', true, false
            ) as sourced_candidate,
            rejection_reasons.rejection_reason_name,
            rejection_reasons.rejection_reason_type,
            job_req.job_status as current_job_req_status,
            iff(
                offers.offer_status = 'accepted',
                datediff('day', applications.applied_at, offers.resolved_at),
                null
            ) as time_to_offer,
            iff(bamboo.hire_date is not null, true, false) as is_hired_in_bamboo
        from applications
        left join
            job_req
            on job_req.application_id = applications.application_id
            and job_req.job_req_to_use = 1
        left join
            greenhouse_departments
            on job_req.department_id = greenhouse_departments.department_id
            and job_req.organization_id = greenhouse_departments.organization_id
        left join
            greenhouse_sources on greenhouse_sources.source_id = applications.source_id
        left join offers on applications.application_id = offers.application_id
        left join candidates on applications.candidate_id = candidates.candidate_id
        left join
            greenhouse_sourcer
            on applications.application_id = greenhouse_sourcer.application_id
        left join
            rejection_reasons
            on rejection_reasons.rejection_reason_id = applications.rejection_reason_id
        left join
            cost_center
            on trim(greenhouse_departments.department_name) = trim(
                cost_center.department
            )
        left join bamboo on bamboo.greenhouse_candidate_id = applications.candidate_id

    )

select *
from final
