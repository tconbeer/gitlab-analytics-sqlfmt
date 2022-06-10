with
    unioned_list as (
        select saas_free_users.*, 'SaaS Free User' as bucket
        from static.sensitive.free_gitlab_com__20200617 as saas_free_users

        UNION

        select saas_paid_users.*, 'SaaS Paid User' as bucket
        from static.sensitive.paid_gitlab_com__20200617 as saas_paid_users

        left join
            static.sensitive.ree_gitlab_com__20200617 as saas_free_users
            on saas_free_users.notification_email = saas_paid_users.notification_email
            and saas_free_users.full_name = saas_paid_users.full_name

        where saas_free_users.notification_email is null

        UNION

        select self_managed_paid_users.*, 'Self-Managed' as bucket
        from
            static.sensitive.self_managed_gitlab_com__20200617
            as self_managed_paid_users

        left join
            static.sensitive.paid_gitlab_com__20200617 as saas_paid_users
            on saas_paid_users.notification_email
            = self_managed_paid_users.notification_email
            and saas_paid_users.full_name = self_managed_paid_users.full_name

        left join
            static.sensitive.free_gitlab_com__20200617 as saas_free_users
            on saas_free_users.notification_email
            = self_managed_paid_users.notification_email
            and saas_free_users.full_name = self_managed_paid_users.full_name

        where
            saas_free_users.notification_email is null
            and saas_paid_users.notification_email is null

    ),
    unioned_list_no_dup_state as (

        select  distinct
            user_id::number as user_id,
            full_name,
            first_name,
            last_name,
            notification_email,
            plan_title,
            state,
            bucket
        from unioned_list
        -- Given a combination of email and email, if there are multiple states, only
        -- pick where active, if not inactive, if not it's only blocked
        qualify
            row_number() over (
                partition by full_name, notification_email, plan_title order by state
            ) = 1

    )

select *
from unioned_list_no_dup_state
