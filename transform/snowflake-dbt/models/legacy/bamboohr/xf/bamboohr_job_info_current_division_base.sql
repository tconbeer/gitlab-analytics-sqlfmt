{{
    config(
        {
            "schema": "legacy",
            "database": env_var("SNOWFLAKE_PROD_DATABASE"),
        }
    )
}}

with
    job_info as (select * from {{ ref("bamboohr_job_info_source") }}),
    bamboo_mapping as (select * from {{ ref("bamboohr_id_employee_number_mapping") }}),
    job_role as (select * from {{ ref("bamboohr_job_role") }}),
    department_name_changes as (

        select
            trim(old_department_name) as old_department_name,
            trim(new_department_name) as new_department_name,
            change_effective_date
        from {{ ref("department_name_changes") }}

    ),
    current_division_department_mapping as (

        select distinct
            division, department, count(bamboo_mapping.employee_id) as total_employees
        from bamboo_mapping
        left join job_info on job_info.employee_id = bamboo_mapping.employee_id
        where
            current_date()
            between effective_date and coalesce(effective_end_date, current_date())
            and bamboo_mapping.termination_date is null
        group by 1, 2
        qualify
            row_number() over (partition by department order by total_employees desc)
            = 1
    -- to account for individuals that have not been transistioned to new division  
    )

select
    job_info.*,
    iff(
        job_info.department = 'Meltano',
        'Engineering',
        coalesce(current_division_department_mapping.division, job_info.division)
    ) as division_mapped_current,
    {{
        bamboohr_division_grouping(
            division="COALESCE(current_division_department_mapping.division, job_info.division)"
        )
    }}
    as division_grouping,
    coalesce(
        department_name_changes.new_department_name, job_info.department
    ) as department_modified,
    {{ bamboohr_department_grouping(department="department_modified") }}
    as department_grouping,
    bamboo_mapping.termination_date
from bamboo_mapping
left join job_info on job_info.employee_id = bamboo_mapping.employee_id
left join
    department_name_changes
    on job_info.department = department_name_changes.old_department_name
left join
    current_division_department_mapping
    on current_division_department_mapping.department = coalesce(
        department_name_changes.new_department_name, job_info.department
    )
