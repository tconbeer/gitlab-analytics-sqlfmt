{{ config(tags=["mnpi_exception"]) }}

with
    namespaces as (

        select creator_id, namespace_id from {{ ref("gitlab_dotcom_namespaces_xf") }}

    ),
    user_preferences as (

        select user_id, setup_for_company
        from {{ ref("gitlab_dotcom_user_preferences") }}

    ),
    memberships as (

        select user_id, ultimate_parent_id, is_billable
        from {{ ref("gitlab_dotcom_memberships") }}

    ),
    users_xf as (

        select user_id, first_name, last_name, users_name, notification_email
        from {{ ref("gitlab_dotcom_users_xf") }}

    ),
    dim_marketing_contact as (

        select gitlab_dotcom_user_id, email_address, dim_crm_account_id
        from {{ ref("dim_marketing_contact") }}

    ),
    dim_crm_account as (

        select crm_account_name, dim_crm_account_id, parent_crm_account_name
        from {{ ref("dim_crm_account") }}

    ),
    is_user_in_company_namespace as (

        select distinct memberships.user_id
        from namespaces
        inner join
            user_preferences
            on user_preferences.user_id = namespaces.creator_id
            and user_preferences.setup_for_company = true
        inner join
            memberships
            on memberships.ultimate_parent_id = namespaces.namespace_id
            and memberships.is_billable = 'TRUE'

    ),
    users as (

        select
            users_xf.user_id as row_integer,
            users_xf.first_name,
            users_xf.last_name,
            users_xf.users_name,
            coalesce(
                users_xf.notification_email, dim_marketing_contact.email_address
            ) as email_id,
            setup_for_company as internal_value1,
            case
                when is_user_in_company_namespace.user_id is not null then 1 else 0
            end as internal_value2,
            dim_crm_account.crm_account_name as company_name,
            dim_crm_account.parent_crm_account_name as parent_company_name,
            case
                when email_id is null
                then 'missing'
                when
                    rlike (
                        substring(
                            email_id,
                            charindex('@', email_id) + 1,
                            len(email_id) - charindex('@', email_id)
                        ),
                        '(yahoo)|(gmail)|(hotmail)|(rediff)|(outlook)|(verizon\\.net)|(live\\.)|(sbcglobal\\.net)|(laposte)|(pm\\.me)|(inbox)|(yandex)|(fastmail)|(protonmail)|(email\\.)|(att\\.net)|(posteo)|(rocketmail)|(bk\\.ru)'
                    ) or substring(
                        email_id,
                        charindex('@', email_id) + 1,
                        len(email_id) - charindex('@', email_id)
                    ) in (
                        'gmail.com',
                        'qq.com',
                        'hotmail.com',
                        '',
                        'yahoo.com',
                        'outlook.com',
                        '163.com',
                        'mail.ru',
                        'googlemail.com',
                        'yandex.ru',
                        'protonmail.com',
                        'icloud.com',
                        't-mobile.com',
                        'example.com',
                        'live.com',
                        '126.com',
                        'me.com',
                        'gmx.de',
                        'hotmail.fr',
                        'web.de',
                        'google.com',
                        'yahoo.fr',
                        'naver.com',
                        'foxmail.com',
                        'aol.com',
                        'msn.com',
                        'hotmail.co.uk',
                        'ya.ru',
                        'wp.pl',
                        'gmx.net',
                        'live.fr',
                        'ymail.com',
                        'orange.fr',
                        'yahoo.co.uk',
                        'ancestry.com',
                        'free.fr',
                        'comcast.net',
                        'hotmail.de',
                        'mail.com',
                        'ukr.net',
                        'yahoo.co.jp',
                        'mac.com',
                        'yahoo.co.in',
                        'gitlab.com',
                        'yahoo.com.br',
                        'gitlab.localhost'
                    )
                then 'personal_email'
                else 'business email'
            end as email_type
        from users_xf
        left join user_preferences on user_preferences.user_id = users_xf.user_id
        left join
            dim_marketing_contact
            on dim_marketing_contact.gitlab_dotcom_user_id = users_xf.user_id
        left join
            dim_crm_account
            on dim_crm_account.dim_crm_account_id
            = dim_marketing_contact.dim_crm_account_id
        left join
            is_user_in_company_namespace
            on is_user_in_company_namespace.user_id = users_xf.user_id


    )
-- -- pulls all business email users or personal email users who have set up for
-- company = True or belongs to a namespace where set up for company is true.
select *
from users
where
    email_type = 'business email' or (
        email_type = 'personal_email' and (
            internal_value1 = true or internal_value2 = 1
        )
    )
