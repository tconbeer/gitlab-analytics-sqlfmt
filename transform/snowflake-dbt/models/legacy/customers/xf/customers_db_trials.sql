{{ config(tags=["mnpi_exception"]) }}

with
    customers as (select * from {{ ref("customers_db_customers_source") }}),
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

        select
            orders_snapshots.*,
            first_value(subscription_name_slugify) ignore nulls over (
                partition by order_id order by order_updated_at asc
            ) as first_subscription_name_slugify
        from orders_snapshots
        left join
            ci_minutes_charges
            on orders_snapshots.subscription_id = ci_minutes_charges.subscription_id
            and orders_snapshots.product_rate_plan_id
            = ci_minutes_charges.product_rate_plan_id
        where ci_minutes_charges.subscription_id is null

    ),
    trials as (

        select
            *,
            first_value(customer_id) over (
                partition by order_id order by order_updated_at desc
            ) as latest_customer_id,
            first_value(gitlab_namespace_id) over (
                partition by order_id order by order_updated_at desc
            ) as latest_namespace_id
        from orders_snapshots
        where order_is_trial = true

    ),
    converted_trials as (

        select distinct
            trials.order_id,
            orders_shapshots_excluding_ci_minutes.first_subscription_name_slugify
            as subscription_name_slugify
        from trials
        inner join
            orders_shapshots_excluding_ci_minutes
            on trials.order_id = orders_shapshots_excluding_ci_minutes.order_id
        inner join
            zuora_subscription_with_positive_mrr_tcv as subscription
            on orders_shapshots_excluding_ci_minutes.first_subscription_name_slugify
            = subscription.subscription_name_slugify
            and trials.order_start_date <= subscription.subscription_start_date
        where
            orders_shapshots_excluding_ci_minutes.subscription_name_slugify is not null

    ),
    joined as (

        select
            trials.order_id,
            trials.latest_namespace_id as gitlab_namespace_id,
            customers.customer_id,


            users.user_id as gitlab_user_id,
            iff(users.user_id is not null, true, false) as is_gitlab_user,
            users.created_at as user_created_at,


            namespaces.created_at as namespace_created_at,
            namespaces.namespace_type,

            iff(converted_trials.order_id is not null, true, false) as is_converted,
            converted_trials.subscription_name_slugify,

            min(order_created_at) as order_created_at,
            min(trials.order_start_date)::date as trial_start_date,
            max(trials.order_end_date)::date as trial_end_date


        from trials
        inner join customers on trials.latest_customer_id = customers.customer_id
        left join namespaces on trials.latest_namespace_id = namespaces.namespace_id
        left join users on customers.customer_provider_user_id = users.user_id
        left join converted_trials on trials.order_id = converted_trials.order_id
        where trials.order_start_date >= '2019-09-01' {{ dbt_utils.group_by(10) }}

    )

select *
from joined
