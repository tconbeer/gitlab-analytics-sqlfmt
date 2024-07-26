{{
    simple_cte(
        [
            ("gitlab_namespaces", "gitlab_dotcom_namespaces_source"),
            ("gitlab_members", "gitlab_dotcom_members_source"),
            ("gitlab_users", "gitlab_dotcom_users_source"),
            ("customer_db_source", "customers_db_customers_source"),
            ("zuora_account", "zuora_account_source"),
            ("zuora_contact", "zuora_contact_source"),
            ("dim_marketing_contact", "dim_marketing_contact"),
        ]
    )
}},
final as (

    select
        dim_marketing_contact_id,
        gitlab_users.notification_email as email_address,
        owner_id as user_id,
        null as customer_db_customer_id,
        namespace_id as namespace_id,
        null as zuora_billing_account_id,
        'Personal Namespace Owner' as marketing_contact_role
    from gitlab_namespaces
    inner join gitlab_users on gitlab_users.user_id = gitlab_namespaces.owner_id
    left join
        dim_marketing_contact
        on dim_marketing_contact.email_address = gitlab_users.notification_email
    where owner_id is not null and namespace_type = 'User' and parent_id is null

    union all

    select distinct
        dim_marketing_contact_id,
        gitlab_users.notification_email as email_address,
        gitlab_users.user_id as user_id,
        null as customer_db_customer_id,
        gitlab_members.source_id as namespace_id,
        null as zuora_billing_account_id,
        'Group Namespace Owner' as marketing_contact_role
    from gitlab_members
    inner join gitlab_users on gitlab_users.user_id = gitlab_members.user_id
    left join
        dim_marketing_contact
        on dim_marketing_contact.email_address = gitlab_users.notification_email
    where
        gitlab_members.member_source_type = 'Namespace'
        and gitlab_members.access_level = 50

    union all

    select distinct
        dim_marketing_contact_id,
        gitlab_users.notification_email as email_address,
        gitlab_users.user_id as user_id,
        null as customer_db_customer_id,
        gitlab_members.source_id as namespace_id,
        null as zuora_billing_account_id,
        'Group Namespace Member' as marketing_contact_role
    from gitlab_members
    inner join gitlab_users on gitlab_users.user_id = gitlab_members.user_id
    left join
        dim_marketing_contact
        on dim_marketing_contact.email_address = gitlab_users.notification_email
    where
        gitlab_members.member_source_type = 'Namespace'
        and gitlab_members.access_level <> 50

    union all

    select
        dim_marketing_contact_id,
        customer_db_source.customer_email as email_address,
        null as user_id,
        customer_id as customer_db_customer_id,
        null as namespace_id,
        cast(null as varchar) as zuora_billing_account_id,
        'Customer DB Owner' as marketing_contact_role
    from customer_db_source
    left join
        dim_marketing_contact
        on dim_marketing_contact.email_address = customer_db_source.customer_email

    union all

    select
        dim_marketing_contact_id,
        zuora_contact.work_email as email_address,
        null as user_id,
        null as customer_db_customer_id,
        null as namespace_id,
        zuora_account.account_id as zuora_billing_account_id,
        'Zuora Billing Contact' as marketing_contact_role
    from zuora_account
    inner join zuora_contact on zuora_contact.account_id = zuora_account.account_id
    left join
        dim_marketing_contact
        on dim_marketing_contact.email_address = zuora_contact.work_email

)


{{
    dbt_audit(
        cte_ref="final",
        created_by="@rmistry",
        updated_by="@jpeguero",
        created_date="2021-01-19",
        updated_date="2022-02-28",
    )
}}
