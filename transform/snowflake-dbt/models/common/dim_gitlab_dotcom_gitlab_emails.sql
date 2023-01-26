{% set column_name = "notification_email_handle" %}

with
    gitlab_dotcom_members as (select * from {{ ref("gitlab_dotcom_members") }}),
    gitlab_dotcom_users as (

        select
            *,
            split_part(notification_email, '@', 0) as notification_email_handle,
            {{ include_gitlab_email(column_name) }} as include_notification_email
        from {{ ref("gitlab_dotcom_users") }}

    ),
    gitlab_dotcom_gitlab_emails_cleaned as (

        select distinct user_id, email_address, email_handle
        from {{ ref("gitlab_dotcom_gitlab_emails") }}
        where length(email_handle) > 1 and include_email_flg = 'Include'

    ),
    sheetload_infrastructure_gitlab_employee as (

        select * from {{ ref("sheetload_infrastructure_missing_employees") }}

    ),
    gitlab_dotcom_team_members_user_id as (

        -- This CTE returns the user_id for any team member in the GitLab.com or
        -- GitLab.org project
        select distinct user_id as gitlab_dotcom_user_id
        from gitlab_dotcom_members
        where
            is_currently_valid = true
            and member_source_type = 'Namespace'
            and source_id in (9970, 6543)  -- 9970 = gitlab-org, 6543 = gitlab-com

    ),
    notification_email as (

        -- This CTE cleans and maps GitLab.com user_name and emails for most GitLab
        -- team members
        -- The email field here is notification_email 
        select distinct
            gitlab_dotcom_user_id,
            user_name,
            case
                when length(gitlab_dotcom_users.notification_email) < 3
                then null
                when gitlab_dotcom_users.include_notification_email = 'Exclude'
                then null
                else gitlab_dotcom_users.notification_email
            end as notification_email
        from gitlab_dotcom_team_members_user_id
        inner join
            gitlab_dotcom_users
            on gitlab_dotcom_team_members_user_id.gitlab_dotcom_user_id
            = gitlab_dotcom_users.user_id
        where user_name not ilike '%admin%'

    ),
    all_known_employee_gitlab_emails as (

        -- This CTE cleans and maps supplemental GitLab.com email addresses from the
        -- `emails` table in gitlab_dotcom, and in the case both are null captures
        -- work email from sheetload
        select
            notification_email.gitlab_dotcom_user_id,
            user_name as gitlab_dotcom_user_name,
            coalesce(
                notification_email.notification_email,
                gitlab_dotcom_gitlab_emails_cleaned.email_address,
                sheetload_infrastructure_gitlab_employee.work_email
            ) as gitlab_dotcom_email_address
        from notification_email
        left join
            gitlab_dotcom_gitlab_emails_cleaned
            on notification_email.gitlab_dotcom_user_id
            = gitlab_dotcom_gitlab_emails_cleaned.user_id
        left join
            sheetload_infrastructure_gitlab_employee
            on notification_email.gitlab_dotcom_user_id
            = sheetload_infrastructure_gitlab_employee.gitlab_dotcom_user_id

    )

select *
from all_known_employee_gitlab_emails
