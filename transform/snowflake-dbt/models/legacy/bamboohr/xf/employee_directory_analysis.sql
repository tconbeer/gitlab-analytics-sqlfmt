{{
    config(
        {
            "materialized": "table",
            "schema": "legacy",
            "database": env_var("SNOWFLAKE_PROD_DATABASE"),
        }
    )
}}

with
    employee_directory_intermediate as (

        select * from {{ ref("employee_directory_intermediate") }}

    ),
    cleaned as (

        select
            date_actual,
            employee_id,
            reports_to,
            full_name,
            work_email,
            gitlab_username,
            job_title,  -- the below case when statement is also used in bamboohr_job_info;
            case
                when division = 'Alliances'
                then 'Alliances'
                when division = 'Customer Support'
                then 'Customer Support'
                when division = 'Customer Service'
                then 'Customer Success'
                when department = 'Data & Analytics'
                then 'Business Operations'
                else nullif(department, '')
            end as department,
            case
                when department = 'Meltano'
                then 'Meltano'
                when division = 'Employee'
                then null
                when division = 'Contractor '
                then null
                when division = 'Alliances'
                then 'Sales'
                when division = 'Customer Support'
                then 'Engineering'
                when division = 'Customer Service'
                then 'Sales'
                else nullif(division, '')
            end as division,
            jobtitle_speciality,
            job_role_modified,
            coalesce(location_factor, hire_location_factor) as location_factor,
            is_hire_date,
            is_termination_date,
            hire_date,
            cost_center,
            layers,
            case
                when date_actual < '2020-06-09'
                then false
                when
                    date_actual >= '2020-06-09'
                    and sales_geo_differential = 'n/a - Comp Calc'
                then false
                else true
            end as exclude_from_location_factor
        from employee_directory_intermediate

    ),
    final as (

        select
            {{ dbt_utils.surrogate_key(["date_actual", "employee_id"]) }} as unique_key,
            cleaned.*
        from cleaned

    )

select distinct *
from final
