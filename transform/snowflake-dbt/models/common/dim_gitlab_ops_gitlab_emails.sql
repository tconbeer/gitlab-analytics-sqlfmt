{% set column_name = "email_handle" %}


with
    gitlab_ops_users_xf as (select * from {{ ref("gitlab_ops_users_xf") }}),
    intermediate as (

        select
            *,
            split_part(notification_email, '@', 0) as email_handle,
            {{ include_gitlab_email(column_name) }} as include_email_flg
        from gitlab_ops_users_xf
        where  -- removes records with just one number  
            length(email_handle) > 1
            and notification_email ilike '%gitlab.com'
            and include_email_flg = 'Include'

    ),
    final as (

        select
            user_id,
            user_name as gitlab_ops_user_name,
            notification_email,
            email_handle,
            count(notification_email) OVER (partition by user_id) as number_of_emails
        from intermediate
        group by 1, 2, 3, 4

    )

select *
from final
