{{ config({"schema": "legacy"}) }}

with
    customers_db_license_seat_links as (

        select * from {{ ref("customers_db_license_seat_links") }}

    ),
    customers_db_orders as (select * from {{ ref("customers_db_orders") }}),
    gitlab_dotcom_gitlab_subscriptions as (

        select * from {{ ref("gitlab_dotcom_gitlab_subscriptions") }}

    ),
    gitlab_dotcom_memberships as (select * from {{ ref("gitlab_dotcom_memberships") }}),
    zuora_rate_plan as (select * from {{ ref("zuora_rate_plan") }}),
    zuora_subscription as (select * from {{ ref("zuora_subscription") }}),
    rate_plans as (

        select subscription_id, array_agg(distinct delivery) as delivery
        from zuora_rate_plan
        where amendement_type != 'RemoveProduct'
        group by 1

    ),
    subscriptions as (

        select
            original_id,
            subscription_id,
            subscription_name,
            subscription_status,
            subscription_start_date::date as subscription_start_date,
            subscription_end_date::date as subscription_end_date
        from zuora_subscription
        where original_id is not null and subscription_status in ('Active', 'Cancelled')

    ),
    zuora as (

        select
            subscriptions.*,
            rate_plans.delivery,
            case
                when array_contains('Self-Managed'::variant, delivery)
                then 'Self-Managed'
                when array_contains('SaaS'::variant, delivery)
                then 'SaaS'
                else 'Others'
            end as delivery_group
        from subscriptions
        inner join
            rate_plans on subscriptions.subscription_id = rate_plans.subscription_id

    ),
    zuora_minus_exceptions as (

        select * from zuora qualify count(*) over (partition by subscription_name) = 1

    ),
    seat_link as (

        select *
        from customers_db_license_seat_links
        qualify
            row_number() over (
                partition by zuora_subscription_name order by report_date desc
            ) = 1

    ),
    self_managed as (

        select
            zuora_minus_exceptions.subscription_name,
            zuora_minus_exceptions.original_id,
            zuora_minus_exceptions.subscription_id,
            zuora_minus_exceptions.subscription_status,
            seat_link.report_date,
            seat_link.active_user_count,
            seat_link.max_historical_user_count,
            seat_link.license_user_count
        from zuora_minus_exceptions
        left join
            seat_link
            on zuora_minus_exceptions.subscription_name
            = seat_link.zuora_subscription_name
        where zuora_minus_exceptions.delivery_group = 'Self-Managed'

    ),
    orders as (

        select
            subscription_name,
            subscription_id,
            product_rate_plan_id,
            gitlab_namespace_id,
            order_start_date,
            order_end_date,
            order_updated_at
        from customers_db_orders
        where
            gitlab_namespace_id is not null
            and order_is_trial = false
            and order_end_date > current_date

    ),
    latest_order_per_subscription_name as (

        select *
        from orders
        qualify
            row_number() over (
                partition by subscription_name
                order by order_end_date desc, order_updated_at desc
            ) = 1

    ),
    customers as (

        select
            zuora_minus_exceptions.*,
            latest_order_per_subscription_name.gitlab_namespace_id
        from zuora_minus_exceptions
        left join
            latest_order_per_subscription_name
            on zuora_minus_exceptions.subscription_name
            = latest_order_per_subscription_name.subscription_name
        where delivery_group = 'SaaS'

    ),
    customers_minus_exceptions as (

        select * from customers qualify count(*) over (partition by subscription_id) = 1

    ),
    gitlab_subscriptions as (

        select
            namespace_id,
            max_seats_used as max_historical_user_count,
            seats as license_user_count
        from gitlab_dotcom_gitlab_subscriptions
        where is_currently_valid = true

    ),
    membership as (

        select
            ultimate_parent_id as namespace_id,
            count(
                distinct case when is_billable = true then user_id end
            ) as active_user_count
        from gitlab_dotcom_memberships
        group by 1

    ),
    saas_seats as (

        select
            gitlab_subscriptions.namespace_id,
            gitlab_subscriptions.max_historical_user_count,
            gitlab_subscriptions.license_user_count,
            membership.active_user_count
        from gitlab_subscriptions
        left join
            membership on gitlab_subscriptions.namespace_id = membership.namespace_id

    ),
    saas as (

        select
            customers_minus_exceptions.subscription_name,
            customers_minus_exceptions.original_id,
            customers_minus_exceptions.subscription_id,
            customers_minus_exceptions.subscription_status,
            current_date() as report_date,
            saas_seats.active_user_count,
            saas_seats.max_historical_user_count,
            saas_seats.license_user_count
        from customers_minus_exceptions
        left join
            saas_seats
            on customers_minus_exceptions.gitlab_namespace_id = saas_seats.namespace_id

    ),
    final as (

        select 'Self-Managed' as delivery_group, self_managed.*
        from self_managed

        UNION

        select 'SaaS' as delivery_group, saas.*
        from saas

    )

select *
from final
