with
    greenhouse_openings as (select * from {{ ref("greenhouse_openings_source") }}),
    greenhouse_jobs as (select * from {{ ref("greenhouse_jobs_source") }}),
    greenhouse_department as (

        select
            department_id,
            department_name,
            case
                when lower(department_name) like '%enterprise sales%'
                then 'Enterprise Sales'
                when lower(department_name) like '%commercial sales%'
                then 'Commercial Sales'
                else trim(department_name)
            end as department_modified
        from {{ ref("greenhouse_departments_source") }}

    ),
    greenhouse_organization as (

        select * from {{ ref("greenhouse_organizations_source") }}

    ),
    greenhouse_opening_custom_fields as (

        select
            opening_id,
            {{
                dbt_utils.pivot(
                    "opening_custom_field",
                    dbt_utils.get_column_values(
                        ref("greenhouse_opening_custom_fields_source"),
                        "opening_custom_field",
                    ),
                    agg="MAX",
                    then_value="opening_custom_field_display_value",
                    else_value="NULL",
                    quote_identifiers=False,
                )
            }}
        from {{ ref("greenhouse_opening_custom_fields_source") }}
        group by opening_id

    ),
    greenhouse_recruiting_xf as (select * from {{ ref("greenhouse_recruiting_xf") }}),
    division_mapping as (

        select distinct
            date_actual, division_mapped_current as division, department as department
        from {{ ref("employee_directory_intermediate") }}

    ),
    cost_center_mapping as (

        select * from {{ ref("cost_center_division_department_mapping_current") }}

    ),
    hires as (select * from {{ ref("employee_directory") }}),
    greenhouse_jobs_offices as (

        select * from {{ ref("greenhouse_jobs_offices_source") }}

    ),
    greenhouse_offices_sources as (

        select *
        from {{ ref("greenhouse_offices_source") }}
        where office_name is not null

    ),
    office as (

        select
            greenhouse_jobs_offices.job_id,
            listagg(distinct office_name, ', ') as office_name
        from greenhouse_jobs_offices
        left join
            greenhouse_offices_sources
            on greenhouse_offices_sources.office_id = greenhouse_jobs_offices.office_id
        where office_name is not null
        group by 1

    ),
    aggregated as (

        select
            greenhouse_openings.job_opening_id,
            greenhouse_openings.job_id,
            greenhouse_opening_custom_fields.finance_id as ghp_id,
            greenhouse_jobs.job_created_at,
            greenhouse_jobs.job_status,
            greenhouse_openings.opening_id,
            greenhouse_recruiting_xf.is_hired_in_bamboo,
            greenhouse_recruiting_xf.candidate_target_hire_date,
            greenhouse_recruiting_xf.offer_status,
            greenhouse_openings.target_start_date,
            greenhouse_openings.job_opened_at as opening_date,
            greenhouse_openings.job_closed_at as closing_date,
            greenhouse_openings.close_reason,
            greenhouse_jobs.job_name as job_title,
            greenhouse_department.department_name,
            coalesce(
                division_mapping.division, cost_center_mapping.division
            ) as division,
            greenhouse_opening_custom_fields.hiring_manager,
            greenhouse_opening_custom_fields.type as opening_type,
            hires.employee_id,
            office.office_name as region
        from greenhouse_openings
        left join greenhouse_jobs on greenhouse_openings.job_id = greenhouse_jobs.job_id
        left join
            greenhouse_department
            on greenhouse_department.department_id = greenhouse_jobs.department_id
        left join
            greenhouse_opening_custom_fields
            on greenhouse_opening_custom_fields.opening_id
            = greenhouse_openings.job_opening_id
        left join
            greenhouse_recruiting_xf
            on greenhouse_openings.hired_application_id
            = greenhouse_recruiting_xf.application_id
        left join
            division_mapping
            on division_mapping.department = greenhouse_department.department_modified
            and division_mapping.date_actual
            = date_trunc(day, greenhouse_openings.job_opened_at)
        left join
            cost_center_mapping
            on cost_center_mapping.department
            = greenhouse_department.department_modified
        left join
            hires
            on hires.greenhouse_candidate_id = greenhouse_recruiting_xf.candidate_id
        left join office on office.job_id = greenhouse_openings.job_id
        where greenhouse_jobs.job_opened_at is not null
    )

select *
from aggregated
