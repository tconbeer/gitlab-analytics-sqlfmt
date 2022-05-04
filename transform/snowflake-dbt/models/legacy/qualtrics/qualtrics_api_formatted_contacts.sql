{{
    config(
        {
            "schema": "sensitive",
            "database": env_var("SNOWFLAKE_PREP_DATABASE"),
        }
    )
}}

with
    users as (select * from {{ ref("gitlab_dotcom_users") }}),
    highest_subscription_plan as (

        select * from {{ ref("gitlab_dotcom_highest_paid_subscription_plan") }}

    ),
    renamed as (

        select
            users.user_id as user_id,
            ifnull(first_name, split_part(users_name, ' ', 1)) as first_name,
            ltrim(
                ifnull(last_name, ltrim(users_name, split_part(users_name, ' ', 1)))
            ) as last_name,
            ifnull(notification_email, public_email) as email_address,
            null as phone_number,
            upper(ifnull(nullif(preferred_language, 'nan'), 'en')) as language,
            decode(
                highest_paid_subscription_plan_id::varchar,
                '1',
                'Early Adopter',
                '2',
                'Bronze',
                '3',
                'Silver',
                '4',
                'Gold',
                '34',
                'Free',
                '67',
                'Default',
                '100',
                'Premium',
                '101',
                'Ultimate',
                '102',
                'Ultimate Trial',
                '103',
                'Premium Trial',
                'Free'
            ) as plan,
            highest_subscription_plan.highest_paid_subscription_namespace_id
            as namespace_id
        from users
        left join
            highest_subscription_plan
            on users.user_id = highest_subscription_plan.user_id

    )

select *
from renamed
where email_address is not null
