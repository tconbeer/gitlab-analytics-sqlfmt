{{ config(tags=["product", "mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("namespaces", "prep_namespace"),
            ("subscriptions", "prep_subscription"),
            ("orders_historical", "dim_order_hist"),
            ("dates", "dim_date"),
            ("product_tiers", "prep_product_tier"),
            ("product_details", "dim_product_detail"),
            ("fct_mrr_with_zero_dollar_charges", "fct_mrr_with_zero_dollar_charges"),
            ("trial_histories", "customers_db_trial_histories_source"),
            ("subscription_delivery_types", "bdg_subscription_product_rate_plan"),
        ]
    )
}},
product_rate_plans as (

    select distinct product_rate_plan_id, dim_product_tier_id, product_tier_name
    from product_details
    where product_delivery_type = 'SaaS'

),
saas_subscriptions as (

    select distinct dim_subscription_id, product_rate_plan_id, dim_product_tier_id
    from subscription_delivery_types
    where product_delivery_type = 'SaaS'

),
trial_tiers as (

    select dim_product_tier_id, product_tier_name
    from product_tiers
    where product_tier_name = 'SaaS - Trial: Ultimate'

),
current_recurring as (

    select distinct fct_mrr_with_zero_dollar_charges.dim_subscription_id
    from fct_mrr_with_zero_dollar_charges
    inner join
        product_details
        on fct_mrr_with_zero_dollar_charges.dim_product_detail_id
        = product_details.dim_product_detail_id
    where
        fct_mrr_with_zero_dollar_charges.dim_date_id
        = {{ get_date_id("DATE_TRUNC('month', CURRENT_DATE)") }}
        and product_details.product_delivery_type = 'SaaS'
        and subscription_status in ('Active', 'Cancelled')

),
namespace_list as (

    select distinct
        namespaces.dim_namespace_id,
        namespaces.namespace_type,
        namespaces.ultimate_parent_namespace_id,
        namespaces.gitlab_plan_id,
        dates.first_day_of_month as namespace_snapshot_month,
        trial_histories.start_date as saas_trial_start_date,
        trial_histories.expired_on as saas_trial_expired_on,
        iff(
            trial_histories.gl_namespace_id is not null
            or (
                namespaces.dim_namespace_id = ultimate_parent_namespace_id
                and namespaces.gitlab_plan_title = 'Ultimate Trial'
            ),
            true,
            false
        ) as namespace_was_trial,
        namespaces.is_currently_valid as is_namespace_active
    from namespaces
    inner join dates on dates.date_actual between namespaces.created_at and current_date
    left join
        trial_histories on namespaces.dim_namespace_id = trial_histories.gl_namespace_id

),
subscription_list as (

    select distinct
        subscriptions.dim_subscription_id,
        subscriptions.dim_subscription_id_original,
        subscriptions.dim_subscription_id_previous,
        subscriptions.subscription_name,
        subscriptions.subscription_name_slugify,
        subscriptions.dim_billing_account_id,
        subscriptions.dim_crm_account_id,
        subscriptions.subscription_start_date,
        subscriptions.subscription_end_date,
        dates.first_day_of_month as subscription_snapshot_month,
        product_rate_plans.product_rate_plan_id as product_rate_plan_id_subscription,
        product_rate_plans.dim_product_tier_id as dim_product_tier_id_subscription,
        product_rate_plans.product_tier_name as product_tier_name_subscription,
        iff(
            current_recurring.dim_subscription_id is not null, true, false
        ) as is_subscription_active
    from subscriptions
    inner join
        dates
        on dates.date_actual between subscriptions.subscription_start_date and ifnull(
            subscriptions.subscription_end_date, current_date
        )
    inner join
        saas_subscriptions
        on subscriptions.dim_subscription_id = saas_subscriptions.dim_subscription_id
    inner join
        product_rate_plans
        on saas_subscriptions.product_rate_plan_id
        = product_rate_plans.product_rate_plan_id
    left join
        current_recurring
        on subscriptions.dim_subscription_id = current_recurring.dim_subscription_id

),
orders as (
    /*
    This CTE transforms orders from the historical orders table in two significant ways:
      1. It corrects for erroneous order start/end dates by substituting in the valid_from/valid_to columns
          when changes are made to the order (generally remapping to renewed subscriptions, new namespaces)
        a. See term_start_date and term_end_date (identifiers borrowed from the Zuora subscription model)
      2. It smooths over same day updates to the namespace linked to a given order,
          which would otherwise result in multiple rows for an order in a given month
        a. See QUALIFY statement below. This gets the last update to an order on a given day
        b. NOTE: This does remove some order-namespace links that existed in the historical orders table
            at one point in time, but a judgement call was made to assume that if the namespace needed
            to be updated within 24 hours it is likely that the previous namespace was incorrect
    */
    select
        orders_historical.dim_order_id,
        orders_historical.customer_id,
        ifnull(
            trial_tiers.dim_product_tier_id, product_rate_plans.dim_product_tier_id
        ) as dim_product_tier_id_with_trial,
        ifnull(
            trial_tiers.product_tier_name, product_rate_plans.product_tier_name
        ) as product_tier_name_with_trial,
        product_rate_plans.dim_product_tier_id as dim_product_tier_id_order,
        product_rate_plans.product_rate_plan_id as product_rate_plan_id_order,
        product_rate_plans.product_tier_name as product_tier_name_order,
        orders_historical.dim_subscription_id as subscription_id_order,
        orders_historical.dim_namespace_id as namespace_id_order,
        min(orders_historical.order_start_date) over (
            partition by orders_historical.dim_order_id
        ) as order_start_date,
        max(orders_historical.order_end_date) over (
            partition by orders_historical.dim_order_id
        ) as order_end_date,
        min(orders_historical.valid_from) over (
            partition by
                orders_historical.dim_order_id,
                orders_historical.dim_subscription_id,
                orders_historical.dim_namespace_id
        ) as term_start_date,
        max(ifnull(orders_historical.valid_to, current_date)) over (
            partition by
                orders_historical.dim_order_id,
                orders_historical.dim_subscription_id,
                orders_historical.dim_namespace_id
        ) as term_end_date,
        orders_historical.order_is_trial,
        iff(order_end_date >= current_date, true, false) as is_order_active
    from orders_historical
    inner join
        product_rate_plans
        on orders_historical.product_rate_plan_id
        = product_rate_plans.product_rate_plan_id
    left join trial_tiers on orders_historical.order_is_trial = true
    where order_start_date is not null
    qualify
        row_number() over (
            partition by
                orders_historical.dim_order_id, orders_historical.valid_from::date
            order by orders_historical.valid_from desc
        )
        = 1

),
order_list as (

    select orders.*, dates.first_day_of_month as order_snapshot_month
    from orders
    inner join
        dates
        on dates.date_actual
        between iff(
            orders.term_start_date < orders.order_start_date,
            orders.order_start_date,
            orders.term_start_date
        ) and iff(
            orders.term_end_date > orders.order_end_date,
            orders.order_end_date,
            orders.term_end_date
        )
    qualify
        row_number() over (
            partition by orders.dim_order_id, dates.first_day_of_month
            order by orders.term_end_date desc
        )
        = 1

),
final as (

    select distinct
        namespace_list.dim_namespace_id,
        subscription_list.dim_subscription_id,
        order_list.dim_order_id,
        coalesce(
            order_list.order_snapshot_month,
            subscription_list.subscription_snapshot_month,
            namespace_list.namespace_snapshot_month
        ) as snapshot_month,
        order_list.namespace_id_order,
        order_list.subscription_id_order,
        namespace_list.ultimate_parent_namespace_id,
        namespace_list.namespace_type,
        namespace_list.is_namespace_active,
        namespace_list.namespace_was_trial,
        namespace_list.saas_trial_start_date,
        namespace_list.saas_trial_expired_on,
        order_list.customer_id,
        order_list.product_rate_plan_id_order,
        order_list.dim_product_tier_id_order,
        order_list.product_tier_name_order,
        order_list.is_order_active,
        order_list.order_start_date,
        order_list.order_end_date,
        order_list.order_is_trial,
        order_list.dim_product_tier_id_with_trial,
        order_list.product_tier_name_with_trial,
        subscription_list.subscription_name,
        subscription_list.subscription_name_slugify,
        subscription_list.dim_subscription_id_original,
        subscription_list.dim_subscription_id_previous,
        subscription_list.dim_billing_account_id,
        subscription_list.dim_crm_account_id,
        subscription_list.is_subscription_active,
        subscription_list.subscription_start_date,
        subscription_list.subscription_end_date,
        subscription_list.product_rate_plan_id_subscription,
        subscription_list.dim_product_tier_id_subscription,
        subscription_list.product_tier_name_subscription,
        case
            when
                namespace_list.gitlab_plan_id in (102, 103)
                and order_list.dim_order_id is null
            then 'Trial Namespace Missing Order'
            when
                order_list.namespace_id_order
                != namespace_list.ultimate_parent_namespace_id
                and namespace_list.is_namespace_active = true
            then 'Order Linked to Non-Ultimate Parent Namespace'
            when
                namespace_list.gitlab_plan_id not in (102, 103)
                and order_list.dim_order_id is null
            then 'Paid Namespace Missing Order'
            when
                namespace_list.gitlab_plan_id not in (102, 103)
                and order_list.subscription_id_order is null
            then 'Paid Namespace Missing Order Subscription'
            when
                namespace_list.gitlab_plan_id not in (102, 103)
                and subscription_list.dim_subscription_id is null
            then 'Paid Namespace Missing Zuora Subscription'
            when
                order_list.subscription_id_order is not null
                and namespace_list.dim_namespace_id is null
            then 'Paid Order Missing Namespace Assignment'
            when
                order_list.subscription_id_order is not null
                and order_list.product_rate_plan_id_order is not null
                and subscription_list.dim_subscription_id is null
            then 'Paid Order Product Rate Plan Misaligned with Zuora'
            when
                order_list.dim_order_id is not null
                and order_list.namespace_id_order is null
            then 'Free Order Missing Namespace Assignment'
            when
                order_list.namespace_id_order is not null
                and namespace_list.dim_namespace_id is null
            then 'Order Namespace Not Found'
            when
                subscription_list.dim_subscription_id is not null
                and order_list.dim_order_id is null
            then 'Paid Subscription Missing Order'
            when
                subscription_list.dim_subscription_id is not null
                and namespace_list.dim_namespace_id is not null
            then 'Paid All Matching'
            when
                namespace_list.gitlab_plan_id in (102, 103)
                and order_list.dim_order_id is not null
            then 'Trial All Matching'
        end as namespace_order_subscription_match_status
    from order_list
    full outer join
        subscription_list
        on order_list.subscription_id_order = subscription_list.dim_subscription_id
        and order_list.product_rate_plan_id_order
        = subscription_list.product_rate_plan_id_subscription
        and order_list.order_snapshot_month
        = subscription_list.subscription_snapshot_month
    full outer join
        namespace_list
        on order_list.namespace_id_order = namespace_list.dim_namespace_id
        and order_list.order_snapshot_month = namespace_list.namespace_snapshot_month

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@ischweickartDD",
        updated_by="@iweeks",
        created_date="2021-06-02",
        updated_date="2022-04-04",
    )
}}
