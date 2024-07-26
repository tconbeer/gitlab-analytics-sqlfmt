with
    dim_date as (select * from {{ ref("dim_date") }}),
    separations as (

        select
            employee_number,
            employee_id,
            hire_date,
            date_actual as separation_date,
            date_trunc(month, date_actual) as separation_month,
            division_mapped_current as division,
            department_modified as department,
            job_title as job_title
        from {{ ref("employee_directory_intermediate") }}
        where is_termination_date = true and date_actual >= '2020-02-01'

    ),
    separation_type as (

        select *
        from {{ ref("bamboohr_employment_status_source") }}
        where lower(employment_status) = 'terminated'

    ),
    eeoc as (select * from {{ ref("bamboohr_id_employee_number_mapping") }}),
    final as (

        select
            dim_date.fiscal_year,
            separations.*,
            termination_type,
            eeoc.gender,
            eeoc.ethnicity,
            eeoc.region
        from separations
        left join dim_date on separations.separation_date = dim_date.date_actual
        left join
            separation_type
            on separations.employee_id = separation_type.employee_id
            and separations.separation_date = separation_type.effective_date
        left join eeoc on separations.employee_id = eeoc.employee_id

    )

select *
from final
