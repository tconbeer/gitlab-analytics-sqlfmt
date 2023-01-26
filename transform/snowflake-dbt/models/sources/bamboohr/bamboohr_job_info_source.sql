with
    source as (

        select *
        from {{ source("bamboohr", "job_info") }}
        order by uploaded_at desc
        limit 1

    ),
    intermediate as (

        select d.value as data_by_row
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    renamed as (

        select
            data_by_row['id']::number as job_id,
            data_by_row['employeeId']::number as employee_id,
            data_by_row['jobTitle']::varchar as job_title,
            data_by_row['date']::date as effective_date,
            data_by_row['department']::varchar as department,
            data_by_row['division']::varchar as division,
            data_by_row['location']::varchar as entity,
            data_by_row['reportsTo']::varchar as reports_to
        from intermediate

    ),
    bamboohr_employment_status as (

        select
            employee_id,
            valid_from_date,
            dateadd(day, 1, valid_from_date) as valid_to_date  -- -adding a day to capture termination date
        from {{ ref("bamboohr_employment_status_xf") }}
        where employment_status = 'Terminated'

    ),
    sheetload_job_roles as (

        select * from {{ source("sheetload", "job_roles_prior_to_2020_02") }}

    ),
    cleaned as (

        select
            job_id,
            renamed.employee_id,
            job_title,
            renamed.effective_date,  -- the below case when statement is also used in employee_directory_analysis until we upgrade to 0.14.0 of dbt
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
            entity,
            reports_to,
            (
                lag(dateadd('day', -1, renamed.effective_date), 1) over (
                    partition by renamed.employee_id
                    order by renamed.effective_date desc, job_id desc
                )
            ) as effective_end_date
        from renamed

    ),
    joined as (

        select
            cleaned.job_id,
            cleaned.employee_id,
            cleaned.job_title,
            cleaned.effective_date,
            coalesce(
                bamboohr_employment_status.valid_to_date, cleaned.effective_end_date
            ) as effective_end_date,
            cleaned.department,
            cleaned.division,
            cleaned.entity,
            cleaned.reports_to,
            sheetload_job_roles.job_role  -- - This will only appear for records prior to 2020-02-28 -- after this data populates in bamboohr_job_role
        from cleaned
        left join
            sheetload_job_roles on sheetload_job_roles.job_title = cleaned.job_title
        left join
            bamboohr_employment_status
            on bamboohr_employment_status.employee_id = cleaned.employee_id
            and bamboohr_employment_status.valid_to_date
            between cleaned.effective_date and coalesce(
                cleaned.effective_end_date, {{ max_date_in_bamboo_analyses() }}
            )

    )

select *
from joined
