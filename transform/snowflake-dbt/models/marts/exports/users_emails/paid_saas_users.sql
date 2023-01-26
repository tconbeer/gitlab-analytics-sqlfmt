with
    users as (select * from {{ source("gitlab_dotcom", "users") }}),
    memberships as (select * from {{ ref("gitlab_dotcom_memberships") }}),
    plans as (select * from {{ ref("gitlab_dotcom_plans") }}),
    zuora_subscription_product_category as (

        -- Return all the subscription information 
        select distinct
            mrr_month,
            account_id,
            account_number,
            crm_id,
            subscription_id,
            product_category,
            delivery
        from {{ ref("zuora_monthly_recurring_revenue") }}

    ),
    zuora_contacts_information as (

        -- Get the Zuora Contact information to check which columns are good 
        select distinct
            contact_id, account_id, first_name, last_name, work_email, personal_email
        from {{ ref("zuora_contact") }}

    ),
    salesforce_contacts_information as (

        -- Get the Salesforce Contact information to check which columns are good 
        select distinct
            account_id,
            contact_id as user_id,
            contact_name as full_name,
            split_part(trim(contact_name), ' ', 1) as first_name,
            array_to_string(
                array_slice(split(trim(contact_name), ' '), 1, 10), ' '
            ) as last_name,
            contact_email as email,
            iff(
                (
                    (inactive_contact = true)
                    or (has_opted_out_email = true)
                    or (invalid_email_address = true)
                    or (email_is_bounced = true)
                ),
                'Inactive',
                'Active'
            ) as state
        from {{ ref("sfdc_contact_source") }}

    ),
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
    saas_paid_users as (

        select distinct
            all_gitlab_user_information.user_id,
            all_gitlab_user_information.full_name,
            all_gitlab_user_information.first_name,
            all_gitlab_user_information.last_name,
            all_gitlab_user_information.notification_email,
            plans.plan_title,
            all_gitlab_user_information.state
        from all_gitlab_user_information
        left join
            memberships on all_gitlab_user_information.user_id = memberships.user_id
        left join
            plans
            on memberships.ultimate_parent_plan_id::varchar = plans.plan_id::varchar
        where memberships.ultimate_parent_plan_id::varchar in ('2', '3', '4')

    ),
    zuora_subscription_product_category_saas_only as (

        -- Filter to Self-Managed subscriptions Only 
        select * from zuora_subscription_product_category where delivery = 'SaaS'

    ),
    zuora_subscription_product_category_saas_only_current_month as (

        select *
        from zuora_subscription_product_category_saas_only
        where mrr_month = date_trunc('month', current_date)

    ),
    zuora_subscription_product_category_saas_only_contacts as (

        -- Get contact information for self-managed subscriptions 
        select distinct
            -- contact_id
            --    AS user_id,
            subscription.subscription_id,
            subscription.account_id,
            first_name || ' ' || last_name as full_name,
            first_name,
            last_name,
            work_email as email,
            subscription.product_category as plan_title,
            iff(
                subscription.account_id in (
                    select account_id
                    from zuora_subscription_product_category_saas_only_current_month
                ),
                'active',
                'inactive'
            ) as state,
            'Zuora Only' as source,
            null as sfdc
        from zuora_contacts_information as contacts
        inner join
            zuora_subscription_product_category_saas_only as subscription
            on contacts.account_id = subscription.account_id

    ),
    zuora_salesforce_subscription_product_category_saas_only_contacts as (

        -- Get contact information for self-managed subscriptions 
        select distinct
            -- contacts.user_id, 
            subscription.subscription_id,
            subscription.account_id,
            contacts.full_name,
            contacts.first_name,
            contacts.last_name,
            contacts.email,
            subscription.product_category as plan_title,

            iff(
                contacts.state = 'inactive'
                or subscription.account_id not in (
                    select account_id
                    from zuora_subscription_product_category_saas_only_current_month
                ),
                'inactive',
                'active'
            ) as state,
            'Zuora to Salesforce' as source,
            contacts.state as sfdc

        from salesforce_contacts_information contacts
        inner join
            zuora_subscription_product_category_saas_only subscription
            on contacts.account_id = subscription.crm_id

    ),
    unioned_data_set as (

        select *
        from zuora_subscription_product_category_saas_only_contacts

        union all

        select *
        from zuora_salesforce_subscription_product_category_saas_only_contacts

    ),
    zuora_sfdc_contacts as (
        select distinct
            null as user_id,
            full_name,
            first_name,
            last_name,
            email as notification_email,
            plan_title,
            state
        from unioned_data_set
        qualify
            row_number() over (
                partition by full_name, email, plan_title order by state asc
            )
            -- If user combination plan is active in one of the systems and inactive
            -- in another one, only consider where active
            = 1

    -- Stops getting contact information from zuora and sfdc
    ),
    zuora_sfdc_contacts_no_gitlab_contacts as (

        select zuora_sfdc_contacts.*
        from zuora_sfdc_contacts
        left join
            saas_paid_users
            on saas_paid_users.notification_email
            = zuora_sfdc_contacts.notification_email
            and saas_paid_users.full_name = zuora_sfdc_contacts.full_name
        where saas_paid_users.notification_email is null

    ),
    zuora_sfdc_gitlab_users as (

        select *
        from zuora_sfdc_contacts_no_gitlab_contacts

        union

        select *
        from saas_paid_users
    )

select distinct *
from zuora_sfdc_gitlab_users
