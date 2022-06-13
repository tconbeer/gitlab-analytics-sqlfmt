{{
    config(
        {
            "schema": "legacy",
            "database": env_var("SNOWFLAKE_PROD_DATABASE"),
        }
    )
}}

with
    hiring_team as (select * from {{ ref("greenhouse_hiring_team_source") }}),
    greenhouse_users as (select * from {{ ref("greenhouse_users_source") }}),
    employees as (select * from {{ ref("employee_directory") }}),
    final as (

        select
            hiring_team.job_id,
            hiring_team.hiring_team_role,
            hiring_team.is_responsible,
            greenhouse_users.employee_id,
            employees.first_name || ' ' || employees.last_name as full_name,
            hiring_team.hiring_team_created_at,
            hiring_team.hiring_team_updated_at
        from hiring_team
        left join greenhouse_users on hiring_team.user_id = greenhouse_users.user_id
        left join employees on employees.employee_number = greenhouse_users.employee_id

    )

select *
from final
