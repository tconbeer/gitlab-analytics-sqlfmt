with
    bamboo_hr_members as (

        select *
        from {{ ref("bamboohr_work_email") }}
        where work_email is not null and rank_email_desc = 1

    ),
    gitlab_dotcom_members as (

        select * from {{ ref("dim_gitlab_dotcom_gitlab_emails") }}

    ),
    gitlab_ops_members as (

        select
            user_id as gitlab_ops_user_id,
            gitlab_ops_user_name,
            notification_email as gitlab_ops_email_address
        from {{ ref("dim_gitlab_ops_gitlab_emails") }}

    ),
    missing_employees as (

        select * from {{ ref("sheetload_infrastructure_missing_employees") }}

    ),
    final as (

        select
            bamboo_hr_members.employee_id as bamboohr_employee_id,
            bamboo_hr_members.full_name as bamboohr_full_name,
            bamboo_hr_members.work_email as bamboohr_gitlab_email,
            coalesce(
                gitlab_dotcom_members.gitlab_dotcom_user_id,
                missing_employees.gitlab_dotcom_user_id
            ) as gitlab_dotcom_user_id,
            gitlab_dotcom_members.gitlab_dotcom_user_name,
            gitlab_ops_user_id,
            gitlab_ops_user_name
        from bamboo_hr_members
        left join
            gitlab_dotcom_members
            on bamboo_hr_members.work_email
            = gitlab_dotcom_members.gitlab_dotcom_email_address
        left join
            gitlab_ops_members
            on bamboo_hr_members.work_email
            = gitlab_ops_members.gitlab_ops_email_address
        left join
            missing_employees
            on bamboo_hr_members.employee_id = missing_employees.employee_id

    )

select *
from final
