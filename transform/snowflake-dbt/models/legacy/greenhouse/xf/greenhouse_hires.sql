with
    applications as (

        select
            *,
            row_number() over (
                partition by candidate_id order by applied_at
            ) as greenhouse_candidate_row_number
        from {{ ref("greenhouse_applications_source") }}
        where application_status = 'hired'

    ),
    offers as (

        select *
        from {{ ref("greenhouse_offers_source") }}
        where offer_status = 'accepted'

    ),
    openings as (select * from {{ ref("greenhouse_openings_source") }}),
    greenhouse_opening_custom_fields as (

        select * from {{ ref("greenhouse_opening_custom_fields") }}

    ),
    bamboo_hires as (select * from {{ ref("employee_directory") }}),
    bamboohr_mapping as (

        select * from {{ ref("bamboohr_id_employee_number_mapping") }}

    ),
    division_department as (select * from {{ ref("employee_directory_intermediate") }}),
    joined as (

        select
            openings.job_id,
            applications.application_id,
            applications.candidate_id,
            bamboo_hires.employee_id,
            bamboo_hires.full_name as employee_name,
            bamboohr_mapping.region,
            offers.start_date as candidate_target_hire_date,
            applications.applied_at,
            applications.greenhouse_candidate_row_number,
            iff(
                applications.greenhouse_candidate_row_number = 1
                and applied_at < bamboo_hires.hire_date,
                bamboo_hires.hire_date,
                candidate_target_hire_date
            ) as hire_date_mod,
            is_hire_date,
            is_rehire_date,
            case
                when greenhouse_candidate_row_number = 1
                then 'hire'
                when offers.start_date = bamboo_hires.rehire_date
                then 'rehire'
                when greenhouse_candidate_row_number > 1
                then 'transfer'
                else null
            end as hire_type,
            greenhouse_opening_custom_fields.job_opening_type,
            division_department.division_mapped_current as division,
            division_department.department_modified as department,
            division_department.employment_status,
            division_department.is_promotion
        from applications
        left join offers on offers.application_id = applications.application_id
        left join
            bamboo_hires
            on bamboo_hires.greenhouse_candidate_id = applications.candidate_id
        left join
            bamboohr_mapping on bamboo_hires.employee_id = bamboohr_mapping.employee_id
        left join
            openings on openings.hired_application_id = applications.application_id
        left join
            greenhouse_opening_custom_fields
            on greenhouse_opening_custom_fields.job_opening_id = openings.job_opening_id
        left join
            division_department
            on division_department.employee_id = bamboo_hires.employee_id
            and division_department.date_actual = iff(
                applications.greenhouse_candidate_row_number = 1
                and applied_at < bamboo_hires.hire_date,
                bamboo_hires.hire_date,
                offers.start_date
            )

    ),
    final as (

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "application_id",
                        "candidate_id",
                    ]
                )
            }} as unique_key,
            job_id,
            application_id,
            candidate_id,
            employee_id,
            employee_name,
            region,
            greenhouse_candidate_row_number,
            hire_date_mod,
            hire_type,
            job_opening_type,
            iff(employment_status is not null, true, false) as hired_in_bamboohr,
            division,
            department
        from joined
        where is_promotion != true  -- removing promotions

    )

select *
from final
