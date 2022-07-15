with
    mapping as (select * from {{ ref("bamboohr_id_employee_number_mapping") }}),
    bamboohr_directory as (

        select *
        from {{ ref("bamboohr_directory_source") }}
        qualify
            row_number() over (partition by employee_id order by uploaded_at desc) = 1

    ),
    department_info as (

        select
            employee_id,
            last_value(job_title) respect nulls over (
                partition by employee_id order by job_id
            ) as last_job_title,
            last_value(reports_to) respect nulls over (
                partition by employee_id order by job_id
            ) as last_supervisor,
            last_value(department) respect nulls over (
                partition by employee_id order by job_id
            ) as last_department,
            last_value(division) respect nulls over (
                partition by employee_id order by job_id
            ) as last_division
        from {{ ref("bamboohr_job_info_source") }}

    ),
    cost_center as (

        select
            employee_id,
            last_value(cost_center) respect nulls over (
                partition by employee_id order by effective_date
            ) as last_cost_center
        from {{ ref("bamboohr_job_role") }}

    ),
    location_factor as (

        select 
      distinct
            bamboo_employee_number,
            first_value(location_factor) over (
                partition by bamboo_employee_number order by valid_from
            ) as hire_location_factor
        from {{ ref("employee_location_factor_snapshots") }}

    ),
    initial_hire as (

        select employee_id, effective_date as hire_date
        from {{ ref("bamboohr_employment_status_source") }}
        where employment_status != 'Terminated'
        qualify row_number() over (partition by employee_id order by effective_date) = 1

    ),
    rehire as (

        select employee_id, is_rehire, valid_from_date as rehire_date
        from {{ ref("bamboohr_employment_status_xf") }}
        where is_rehire = 'True'

    ),
    final as (

        select 
      distinct
            mapping.employee_id,
            mapping.employee_number,
            mapping.first_name,
            mapping.last_name,
            mapping.first_name || ' ' || mapping.last_name as full_name,
            bamboohr_directory.work_email as last_work_email,
            iff(
                rehire.is_rehire = 'True', initial_hire.hire_date, mapping.hire_date
            ) as hire_date,
            rehire.rehire_date,
            mapping.termination_date,
            department_info.last_job_title,
            department_info.last_supervisor,
            department_info.last_department,
            department_info.last_division,
            cost_center.last_cost_center,
            location_factor.hire_location_factor,
            mapping.greenhouse_candidate_id
        from mapping
        left join
            bamboohr_directory on bamboohr_directory.employee_id = mapping.employee_id
        left join department_info on mapping.employee_id = department_info.employee_id
        left join
            location_factor
            on location_factor.bamboo_employee_number = mapping.employee_number
        left join initial_hire on initial_hire.employee_id = mapping.employee_id
        left join rehire on rehire.employee_id = mapping.employee_id
        left join cost_center on cost_center.employee_id = mapping.employee_id
        where mapping.hire_date < date_trunc('week', dateadd(week, 3, current_date))

    )

select *
from final
