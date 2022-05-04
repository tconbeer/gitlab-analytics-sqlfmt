{{
    config(
        {
            "schema": "legacy",
            "database": env_var("SNOWFLAKE_PROD_DATABASE"),
        }
    )
}}

with
    employees as (

        select *
        from {{ ref("employee_directory") }}
        where termination_date is null and hire_date <= current_date()

    ),
    contacts as (select * from {{ ref("bamboohr_emergency_contacts_source") }}),
    contacts_aggregated as (

        select
            employee_id,
            sum(
                iff(
                    home_phone is not null
                    or mobile_phone is not null
                    or work_phone is not null,
                    1,
                    0
                )
            ) as total_emergency_contact_numbers
        from contacts
        group by 1

    ),
    final as (

        select
            employees.employee_id,
            employees.full_name,
            employees.hire_date,
            employees.last_work_email,
            coalesce(
                contacts_aggregated.total_emergency_contact_numbers, 0
            ) as total_emergency_contacts
        from employees
        left join
            contacts_aggregated
            on employees.employee_id = contacts_aggregated.employee_id

    )

select *
from final
where total_emergency_contacts = 0
