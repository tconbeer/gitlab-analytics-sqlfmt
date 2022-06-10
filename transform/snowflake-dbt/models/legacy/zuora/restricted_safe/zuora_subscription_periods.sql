with
    zuora_account as (select * from {{ ref("zuora_account") }})

    ,
    zuora_rate_plan as (select * from {{ ref("zuora_rate_plan") }})

    ,
    zuora_rate_plan_charge as (select * from {{ ref("zuora_rate_plan_charge") }})

    ,
    zuora_subscription as (select * from {{ ref("zuora_subscription") }})

    ,
    subscription_joined_with_accounts as (

        select distinct
            zuora_subscription.subscription_id,
            zuora_subscription.subscription_name,
            zuora_subscription.subscription_name_slugify,
            zuora_subscription.subscription_status,
            zuora_subscription.version,
            zuora_subscription.zuora_renewal_subscription_name_slugify,
            zuora_subscription.renewal_term,
            zuora_subscription.renewal_term_period_type,

            zuora_subscription.account_id,
            zuora_account.account_number,
            zuora_account.account_name,
            subscription_start_date,
            term_start_date as subscription_version_term_start_date,
            term_end_date as subscription_version_term_end_date,
            min(term_start_date) over (
                partition by subscription_name_slugify
                order by zuora_subscription.version desc
                rows between unbounded preceding and 1 preceding
            ) as min_following_subscription_version_term_start_date
        from zuora_subscription
        inner join
            zuora_account on zuora_subscription.account_id = zuora_account.account_id

    )

    ,
    subscription_with_valid_auto_renew_setting as (

        /* 
      Specific CTE to check auto_renew settings before the renewal happens.
      This is a special case for auto-reneweable subscriptions with a failing payment.
      A good example is where subscription_name_slugify = 'a-s00014110'.
    */
        select distinct
            subscription_name_slugify,
            term_start_date,
            term_end_date,
            last_value(auto_renew_native_hist) over (
                partition by subscription_name_slugify, term_start_date, term_end_date
                order by version
            ) as last_auto_renew
        from zuora_subscription
        /* 
      When a subscription has auto-renew turned on but the CC is declined, a new version of the same
      subscription is created (same term_end_date) but the created_date is after the term_end_date.
      This new version has auto_column set to FALSE.
    */
        where created_date < term_end_date

    )

    ,
    subscription_joined_with_charges as (

        select distinct
            subscription_joined_with_accounts.subscription_id,
            subscription_joined_with_accounts.subscription_name,
            subscription_joined_with_accounts.subscription_name_slugify,
            subscription_joined_with_accounts.subscription_status,
            subscription_joined_with_accounts.version,
            subscription_joined_with_accounts.zuora_renewal_subscription_name_slugify,
            get(
                subscription_joined_with_accounts.zuora_renewal_subscription_name_slugify,
                0
            )::varchar as zuora_next_renewal_subscription_name_slugify,
            subscription_joined_with_accounts.account_id,
            subscription_joined_with_accounts.account_number,
            subscription_joined_with_accounts.account_name,
            subscription_joined_with_accounts.subscription_start_date,
            subscription_joined_with_accounts.subscription_version_term_start_date,
            subscription_joined_with_accounts.subscription_version_term_end_date,
            last_value(product_category) over (
                partition by subscription_joined_with_accounts.subscription_id
                order by zuora_rate_plan_charge.effective_start_date
            ) as latest_product_category,
            {{ delivery("latest_product_category", "latest_delivery") }},
            last_value(mrr) over (
                partition by subscription_joined_with_accounts.subscription_id
                order by zuora_rate_plan_charge.effective_start_date
            ) as mrr,
            sum(tcv) over (
                partition by subscription_joined_with_accounts.subscription_id
            ) as tcv
        from subscription_joined_with_accounts
        inner join
            zuora_rate_plan
            on subscription_joined_with_accounts.subscription_id
            = zuora_rate_plan.subscription_id
        inner join
            zuora_rate_plan_charge
            on zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
            -- remove refunded subscriptions
            and mrr > 0
            and tcv > 0
        where
            (
                subscription_version_term_start_date
                < min_following_subscription_version_term_start_date
                or min_following_subscription_version_term_start_date is null
            )
            -- remove cancelled subscription
            and subscription_version_term_start_date
            != subscription_version_term_end_date

    )

    ,
    subscription_with_renewals as (

        /* 
    select the next renewal subscription name slugify, 
    look up the mrr of the subscription period that comes right after (the one with the lowest version number)
    account_id = '2c92a0ff55a0e4940155c01a0ab36854' is a good example
    */
        select distinct
            subscription_joined_with_charges.subscription_id,
            subscription_joined_with_charges.subscription_name_slugify,
            first_value(renewed_subscription.mrr) over (
                partition by subscription_joined_with_charges.subscription_name_slugify
                order by renewed_subscription.version
            ) as mrr_from_renewal_subscription
        from subscription_joined_with_charges
        inner join
            subscription_joined_with_charges as renewed_subscription
            on subscription_joined_with_charges.zuora_next_renewal_subscription_name_slugify
            = renewed_subscription.subscription_name_slugify

    )

select
    subscription_joined_with_charges.*,
    coalesce(
        subscription_with_valid_auto_renew_setting.last_auto_renew, false
    ) as has_auto_renew_on,
    case
        -- manual linked subscription
        when
            subscription_joined_with_charges.zuora_renewal_subscription_name_slugify
            is
            not null
        then true
        -- new version available, got renewed
        when
            lead(subscription_joined_with_charges.subscription_name_slugify) over (
                partition by subscription_joined_with_charges.subscription_name_slugify
                order by version
            ) is not null
        then true
        else false
    end as is_renewed,
    coalesce(
        lead(subscription_joined_with_charges.mrr) over (
            partition by subscription_joined_with_charges.subscription_name_slugify
            order by version
        ),
        subscription_with_renewals.mrr_from_renewal_subscription,
        0
    ) as mrr_from_renewal_subscription
from subscription_joined_with_charges
left join
    subscription_with_valid_auto_renew_setting
    on subscription_joined_with_charges.subscription_name_slugify
    = subscription_with_valid_auto_renew_setting.subscription_name_slugify
    and subscription_joined_with_charges.subscription_version_term_start_date
    = subscription_with_valid_auto_renew_setting.term_start_date
    and subscription_joined_with_charges.subscription_version_term_end_date
    = subscription_with_valid_auto_renew_setting.term_end_date
left join
    subscription_with_renewals
    on subscription_joined_with_charges.subscription_id
    = subscription_with_renewals.subscription_id
order by subscription_start_date, version
