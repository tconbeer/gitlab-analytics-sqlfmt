{{
    config(
        {
            "schema": "legacy",
            "database": env_var("SNOWFLAKE_PROD_DATABASE"),
        }
    )
}}

with
    employees as (select * from {{ ref("employee_directory_analysis") }}),
    bamboohr_engineering_division_mapping as (

        select * from {{ ref("sheetload_product_group_mappings") }}

    ),
    engineering_employees as (

        select
            date_actual,
            employee_id,
            full_name,
            job_title as job_title,
            trim(
                substring(
                    lower(trim(value::varchar)),
                    charindex(':', lower(trim(value::varchar))) + 1,
                    100
                )
            ) as job_title_speciality,
            reports_to,
            layers,
            department,
            work_email
        from
            employees,
            lateral flatten(
                input => split(
                    coalesce(replace(jobtitle_speciality, '&', ','), ''), ','
                )
            )
        where division = 'Engineering' and date_actual >= '2020-01-01'

    ),
    engineering_employee_attributes as (

        select
            engineering_employees.date_actual,
            engineering_employees.employee_id,
            engineering_employees.full_name,
            engineering_employees.job_title,
            case
                when bamboohr_engineering_division_mapping.section_name = 'sec'
                then 'secure'
                else bamboohr_engineering_division_mapping.section_name
            end as sub_department,
            engineering_employees.job_title_speciality,
            case
                when
                    engineering_employees.employee_id in (
                        41965,
                        41996,
                        41453,
                        41482,
                        41974,
                        41487,
                        42029,
                        40914,
                        41954,
                        46
                    ) or lower(engineering_employees.job_title) like '%backend%'
                then 'backend'
                when lower(engineering_employees.job_title) like '%fullstack%'
                then 'fullstack'
                when lower(engineering_employees.job_title) like '%frontend%'
                then 'frontend'
                else null
            end as technology_group,
            engineering_employees.department,
            engineering_employees.work_email,
            engineering_employees.reports_to
        from engineering_employees
        left join
            bamboohr_engineering_division_mapping
            on bamboohr_engineering_division_mapping.group_name
            = engineering_employees.job_title_speciality

    )

select *
from engineering_employee_attributes
