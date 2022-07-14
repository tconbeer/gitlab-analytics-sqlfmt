{{ config(tags=["mnpi_exception"]) }}

with
    customers as (select * from {{ ref("customers_db_customers") }}),
    customers_db_latest_trial_per_namespace as (

        select * from {{ ref("customers_db_latest_trial_per_namespace") }}),
    gitlab_subscriptions as (

        select * from {{ ref("gitlab_dotcom_gitlab_subscriptions_snapshots_base") }}),
    namespaces as (select * from {{ ref("gitlab_dotcom_namespaces") }}),
    orders_snapshots as (select * from {{ ref("customers_db_orders_snapshots_base") }}),
    users as (select * from {{ ref("gitlab_dotcom_users") }}),
    zuora_rate_plan as (select * from {{ ref("zuora_rate_plan") }}),
    zuora_base_mrr as (select * from {{ ref("zuora_base_mrr") }}),
    zuora_subscription_with_positive_mrr_tcv as (

        select distinct subscription_name_slugify, subscription_start_date
        from zuora_base_mrr

    ),
    ci_minutes_charges as (

        select * from zuora_rate_plan where rate_plan_name = '1,000 CI Minutes'),
    orders_shapshots_excluding_ci_minutes as (

        select orders_snapshots.*
        from orders_snapshots
        left join
            ci_minutes_charges
            on orders_snapshots.subscription_id = ci_minutes_charges.subscription_id
            and orders_snapshots.product_rate_plan_id
            = ci_minutes_charges.product_rate_plan_id
        where ci_minutes_charges.subscription_id is null

    ),
    namespace_with_latest_trial_date as (

        select
            namespace_id,
            max(gitlab_subscription_trial_ends_on) as latest_trial_end_date,
            dateadd(
                'day', -30, max(gitlab_subscription_trial_ends_on)
            ) as estimated_latest_trial_start_date
        from gitlab_subscriptions
        where gitlab_subscription_trial_ends_on is not null
        group by 1

    ),
    trials_joined as (

        select
            namespace_with_latest_trial_date.namespace_id,
            namespace_with_latest_trial_date.latest_trial_end_date,
            coalesce(
                customers_db_latest_trial_per_namespace.order_start_date,
                namespace_with_latest_trial_date.estimated_latest_trial_start_date
            ) as latest_trial_start_date,
            customers.customer_id,
            customers.customer_provider_user_id,
            customers.country,
            customers.company_size

        from namespace_with_latest_trial_date
        left join
            customers_db_latest_trial_per_namespace
            on namespace_with_latest_trial_date.namespace_id
            = customers_db_latest_trial_per_namespace.gitlab_namespace_id
        left join
            customers
            on customers_db_latest_trial_per_namespace.customer_id
            = customers.customer_id

    ),
    converted_trials as (

        select distinct
            trials_joined.namespace_id,
            orders_shapshots_excluding_ci_minutes.subscription_name_slugify,
            subscription.subscription_start_date
        from trials_joined
        inner join
            orders_shapshots_excluding_ci_minutes
            on trials_joined.namespace_id
            = try_to_number(orders_shapshots_excluding_ci_minutes.gitlab_namespace_id)
        inner join
            zuora_subscription_with_positive_mrr_tcv as subscription
            on orders_shapshots_excluding_ci_minutes.subscription_name_slugify
            = subscription.subscription_name_slugify
            and trials_joined.latest_trial_start_date
            <= subscription.subscription_start_date
        where
            orders_shapshots_excluding_ci_minutes.subscription_name_slugify is not null

    ),
    joined as (

        select
            trials_joined.namespace_id,
            trials_joined.customer_id,
            trials_joined.country,
            trials_joined.company_size,

            users.user_id as gitlab_user_id,
            iff(users.user_id is not null, true, false) as is_gitlab_user,
            users.created_at as user_created_at,

            namespaces.created_at as namespace_created_at,
            namespaces.namespace_type,

            trials_joined.latest_trial_start_date,
            trials_joined.latest_trial_end_date,
            min(subscription_start_date) as subscription_start_date
        from trials_joined
        left join namespaces on trials_joined.namespace_id = namespaces.namespace_id
        left join users on trials_joined.customer_provider_user_id = users.user_id
        left join
            converted_trials
            on trials_joined.namespace_id = converted_trials.namespace_id
            {{ dbt_utils.group_by(11) }}

    )

select *
from joined
