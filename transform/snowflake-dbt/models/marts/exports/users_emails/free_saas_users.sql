with
    users as (select * from {{ source("gitlab_dotcom", "users") }}),
    memberships as (select * from {{ ref("gitlab_dotcom_memberships") }}),
    plans as (select * from {{ ref("gitlab_dotcom_plans") }}),
    all_gitlab_user_information as (
        select
            id as user_id,
            trim(name) as full_name,
            split_part(trim(name), ' ', 1) as first_name,
            array_to_string(
                array_slice(split(trim(name), ' '), 1, 10), ' '
            ) as last_name,
            username,
            notification_email,
            state
        from users
        qualify row_number() over (partition by id order by updated_at desc) = 1

    ),
    saas_free_users as (

        select distinct
            all_gitlab_user_information.user_id,
            all_gitlab_user_information.full_name,
            all_gitlab_user_information.first_name,
            all_gitlab_user_information.last_name,
            all_gitlab_user_information.notification_email,
            decode(
                memberships.ultimate_parent_plan_id,
                '34',
                'Free',
                'trial',
                'Free Trial',
                'Free'
            ) as plan_title,
            all_gitlab_user_information.state
        from all_gitlab_user_information
        left join
            memberships on all_gitlab_user_information.user_id = memberships.user_id
        where memberships.ultimate_parent_plan_id::varchar not in ('2', '3', '4')

    )

select *
from saas_free_users
