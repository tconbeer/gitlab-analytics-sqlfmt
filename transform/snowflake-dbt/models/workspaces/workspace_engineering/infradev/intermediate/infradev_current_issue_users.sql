{{ config(materialized="ephemeral") }}

with
    issue_assignees as (

        select * from {{ ref("gitlab_dotcom_issue_assignees_source") }}

    ),
    users as (select * from {{ ref("gitlab_dotcom_users_dedupe_source") }}),
    assigend_users as (

        select
            issue_id as dim_issue_id,
            listagg(distinct users.username, ', ') as assigned_usernames
        from issue_assignees
        left join users on issue_assignees.user_id = users.id
        group by 1
    )

select *
from assigend_users
