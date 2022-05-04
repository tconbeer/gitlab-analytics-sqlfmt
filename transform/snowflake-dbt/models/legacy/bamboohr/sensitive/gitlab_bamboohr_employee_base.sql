with
    employee_directory as (select * from {{ ref("employee_directory_intermediate") }}),
    gitlab_mapping as (

        select * from {{ ref("map_team_member_bamboo_gitlab_dotcom_gitlab_ops") }}

    ),
    sheetload_missing as (

        select * from {{ ref("sheetload_infrastructure_missing_employees") }}

    ),
    intermediate as (

        select distinct
            date_trunc(month, date_actual) as month_date,
            employee_directory.date_actual as valid_from,
            employee_directory.employee_id,
            employee_directory.full_name,
            division,
            department,
            jobtitle_speciality,
            job_role,
            reports_to,
            coalesce(
                gitlab_mapping.gitlab_dotcom_user_id,
                sheetload_missing.gitlab_dotcom_user_id
            ) as gitlab_dotcom_user_id,
            gitlab_ops_user_id
        from employee_directory
        left join
            gitlab_mapping
            on employee_directory.employee_id = gitlab_mapping.bamboohr_employee_id
        left join
            sheetload_missing
            on employee_directory.employee_id = sheetload_missing.employee_id
        qualify
            row_number() over (
                partition by
                    date_trunc(month, date_actual),
                    employee_directory.employee_id,
                    employee_directory.division,
                    employee_directory.department,
                    jobtitle_speciality,
                    job_role_modified,
                    reports_to
                order by employee_directory.date_actual
            ) = 1

    ),
    final as (

        select
            month_date,
            valid_from,
            coalesce(
                lead(dateadd(day, -1, valid_from)) over (
                    partition by employee_id order by valid_from
                ),
                last_day(valid_from)
            ) as valid_to,
            employee_id,
            full_name,
            division,
            department,
            jobtitle_speciality,
            job_role,
            reports_to,
            gitlab_dotcom_user_id,
            gitlab_ops_user_id,
            datediff(day, valid_from, valid_to) as total_days
        from intermediate
    -- need to account for terminations
    )

select *
from final
