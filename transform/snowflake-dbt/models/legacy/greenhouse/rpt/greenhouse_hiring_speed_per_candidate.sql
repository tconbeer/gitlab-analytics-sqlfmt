with
    offers as (

        select *
        from {{ ref("greenhouse_recruiting_xf") }}
        where offer_status is not null

    ),
    candidate_names as (select * from {{ source("greenhouse", "candidates") }}),
    hires as (select * from {{ ref("greenhouse_hires") }}),
    location_factor as (select * from {{ ref("employee_directory_analysis") }})

select
    offers.offer_status as offer_status,
    offers.application_status as application_status,
    hire_date_mod as hire_date,
    (candidate_names.first_name || ' ' || candidate_names.last_name) as candidate_name,
    offers.division_modified as division,
    offers.department_name as department,
    offers.candidate_recruiter,
    offers.job_id,
    offers.job_name as vacancy,
    offers.time_to_offer,
    offers.source_name as source,
    application_date,
    offer_sent_date as offer_sent,
    offer_resolved_date as offer_accept,
    hires.region as location,
    location_factor.location_factor,
    offers.candidate_id
from offers
left join candidate_names on candidate_names.id = offers.candidate_id
left join
    hires
    on hires.candidate_id = offers.candidate_id
    and hires.application_id = offers.application_id
left join
    location_factor
    on location_factor.date_actual = hires.hire_date_mod
    and hires.employee_id = location_factor.employee_id
where
    date_trunc(month, offer_sent)
    between date_trunc(month, dateadd(month, -6, current_date())) and current_date()
