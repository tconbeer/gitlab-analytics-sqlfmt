with
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
    zuora_subscription_product_category_self_managed_only as (

        -- Filter to Self-Managed subscriptions Only 
        select *
        from zuora_subscription_product_category
        where delivery = 'Self-Managed'

    ),
    zuora_subscription_product_category_self_managed_only_current_month as (

        select *
        from zuora_subscription_product_category_self_managed_only
        where mrr_month = date_trunc('month', current_date)

    ),
    zuora_subscription_product_category_self_managed_only_contacts as (

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
                    from
                        zuora_subscription_product_category_self_managed_only_current_month
                ),
                'active',
                'inactive'
            ) as state,
            'Zuora Only' as source
        from zuora_contacts_information contacts
        inner join
            zuora_subscription_product_category_self_managed_only subscription
            on contacts.account_id = subscription.account_id

    ),
    zuora_salesforce_subscription_product_category_self_managed_only_contacts as (

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
                    from
                        zuora_subscription_product_category_self_managed_only_current_month
                ),
                'inactive',
                'active'
            ) as state,
            'Zuora to Salesforce' as source

        from salesforce_contacts_information contacts
        inner join
            zuora_subscription_product_category_self_managed_only subscription
            on contacts.account_id = subscription.crm_id

    ),
    unioned_data_set as (

        select *
        from zuora_subscription_product_category_self_managed_only_contacts

        union all

        select *
        from zuora_salesforce_subscription_product_category_self_managed_only_contacts

    ),
    final as (

        select distinct
            null as user_id,
            full_name,
            first_name,
            last_name,
            email as notification_email,
            plan_title,
            state
        from unioned_data_set
        -- If user combination plan is active in one of the systems and inactive in
        -- another one, only consider where active
        qualify
            row_number() over (
                partition by full_name, email, plan_title order by state asc
            )
            = 1

    )

select *
from final
