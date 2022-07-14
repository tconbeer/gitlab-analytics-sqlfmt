{{ config(tags=["product", "mnpi_exception"]) }}

{{
    simple_cte(
        [
            ("subscriptions", "prep_subscription"),
            ("orders", "customers_db_orders_source"),
            ("product_tiers", "prep_product_tier"),
            ("product_details", "dim_product_detail"),
            ("fct_mrr_with_zero_dollar_charges", "fct_mrr_with_zero_dollar_charges"),
            ("subscription_delivery_types", "bdg_subscription_product_rate_plan"),
        ]
    )
}},
trial_tiers as (

    select dim_product_tier_id, product_tier_name
    from product_tiers
    where product_tier_name = 'Self-Managed - Trial: Ultimate'

),
product_rate_plans as (

    select distinct product_rate_plan_id, dim_product_tier_id, product_tier_name
    from product_details
    where product_delivery_type = 'Self-Managed'

),
sm_subscriptions as (

    select distinct dim_subscription_id, product_rate_plan_id, dim_product_tier_id
    from subscription_delivery_types
    where product_delivery_type = 'Self-Managed'

),
current_recurring as (

    select distinct
        fct_mrr_with_zero_dollar_charges.dim_subscription_id,
        product_details.product_rate_plan_id,
        product_details.dim_product_tier_id
    from fct_mrr_with_zero_dollar_charges
    inner join
        product_details
        on fct_mrr_with_zero_dollar_charges.dim_product_detail_id
        = product_details.dim_product_detail_id
        and product_details.product_delivery_type = 'Self-Managed'
    where
        fct_mrr_with_zero_dollar_charges.dim_date_id
        = {{ get_date_id("DATE_TRUNC('month', CURRENT_DATE)") }}
        and subscription_status in ('Active', 'Cancelled')

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
        product_rate_plans.product_rate_plan_id as product_rate_plan_id_subscription,
        product_rate_plans.dim_product_tier_id as dim_product_tier_id_subscription,
        product_rate_plans.product_tier_name as product_tier_name_subscription,
        count(*) over (
            partition by subscriptions.dim_subscription_id
        ) as count_of_tiers_per_subscription,
        iff(
            current_recurring.dim_subscription_id is not null, true, false
        ) as is_subscription_active
    from subscriptions
    inner join
        sm_subscriptions
        on subscriptions.dim_subscription_id = sm_subscriptions.dim_subscription_id
    inner join
        product_rate_plans
        on sm_subscriptions.product_rate_plan_id
        = product_rate_plans.product_rate_plan_id
    left join
        current_recurring
        on sm_subscriptions.dim_subscription_id = current_recurring.dim_subscription_id

),
order_list as (

    select
        orders.order_id,
        orders.customer_id,
        ifnull(
            trial_tiers.dim_product_tier_id, product_rate_plans.dim_product_tier_id
        ) as dim_product_tier_id_with_trial,
        ifnull(
            trial_tiers.product_tier_name, product_rate_plans.product_tier_name
        ) as product_tier_name_with_trial,
        product_rate_plans.dim_product_tier_id as dim_product_tier_id_order,
        product_rate_plans.product_rate_plan_id as product_rate_plan_id_order,
        product_rate_plans.product_tier_name as product_tier_name_order,
        orders.subscription_id as subscription_id_order,
        orders.subscription_name as subscription_name_order,
        orders.subscription_name_slugify as subscription_name_slugify_order,
        orders.order_start_date,
        orders.order_end_date,
        orders.order_is_trial,
        iff(
            ifnull(orders.order_end_date, current_date) >= current_date, true, false
        ) as is_order_active
    from orders
    inner join
        product_rate_plans
        on orders.product_rate_plan_id = product_rate_plans.product_rate_plan_id
    left join trial_tiers on orders.order_is_trial = true
    where orders.order_start_date is not null

),
final as (

    select
        subscription_list.dim_subscription_id,
        order_list.order_id,
        order_list.subscription_id_order,
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
                order_list.product_tier_name_with_trial
                = 'Self-Managed - Trial: Ultimate'
            then 'Trial Order'
            when
                order_list.order_id is not null
                and order_list.subscription_id_order is null
            then 'Order Subscription Not Found'
            when
                order_list.subscription_id_order is not null
                and subscription_list.dim_subscription_id is null
            then 'Paid Order Product Rate Plan Misaligned with Zuora'
            when
                subscription_list.dim_subscription_id is not null
                and order_list.order_id is null
            then 'Paid Subscription Missing Order'
            when
                subscription_list.dim_subscription_id is not null
                and order_list.order_id is not null
            then 'Paid All Matching'
        end as order_subscription_match_status
    from order_list
    full outer join
        subscription_list
        on order_list.subscription_id_order = subscription_list.dim_subscription_id
        and order_list.product_rate_plan_id_order
        = subscription_list.product_rate_plan_id_subscription

)

{{
    dbt_audit(
        cte_ref="final",
        created_by="@ischweickartDD",
        updated_by="@iweeks",
        created_date="2021-02-02",
        updated_date="2022-04-04",
    )
}}
