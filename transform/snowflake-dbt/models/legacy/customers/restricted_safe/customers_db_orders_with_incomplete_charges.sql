with
    customers as (select * from {{ ref("customers_db_customers_source") }}),
    orders_snapshots as (select * from {{ ref("customers_db_orders_snapshots_base") }}),
    orders_with_valid_charges as (

        select * from {{ ref("customers_db_orders_with_valid_charges") }}

    ),
    trials as (select * from {{ ref("customers_db_trials") }}),
    zuora_rp as (select * from {{ ref("zuora_rate_plan") }}),
    zuora_rpc as (select * from {{ ref("zuora_rate_plan_charge") }}),
    zuora_subscription_xf as (select * from {{ ref("zuora_subscription_xf") }}),
    orders_with_subscriptions_without_product_plan_rate as (

        select distinct
            order_id,
            subscription_id,
            subscription_name_slugify,
            customer_id,
            gitlab_namespace_id,
            product_rate_plan_id,
            first_value(order_created_at) over (
                partition by order_id order by valid_from asc
            ) as order_created_at,
            first_value(order_updated_at) over (
                partition by order_id order by valid_to asc
            ) as order_updated_at
        from orders_snapshots
        where
            orders_snapshots.product_rate_plan_id is null
            and orders_snapshots.order_is_trial = false
            and orders_snapshots.subscription_id is not null

    ),
    joined as (

        select distinct
            zuora_rpc.rate_plan_charge_id,

            -- Foreign Keys
            opr.customer_id,
            opr.gitlab_namespace_id,
            opr.subscription_name_slugify,
            zuora_rp.rate_plan_id,

            -- Financial Info
            iff(
                zuora_rpc.created_by_id in (
                    '2c92a0fd55822b4d015593ac264767f2',
                    '2c92a0107bde3653017bf00cd8a86d5a'
                ),
                true,
                false
            ) as is_purchased_through_subscription_portal,

            -- Orders metadata
            first_value(opr.customer_id) over (
                partition by opr.subscription_name_slugify
                order by opr.order_updated_at desc
            ) as current_customer_id,
            first_value(opr.gitlab_namespace_id) over (
                partition by opr.subscription_name_slugify
                order by
                    opr.gitlab_namespace_id is not null desc, opr.order_updated_at desc
            ) as current_gitlab_namespace_id,
            first_value(opr.customer_id) over (
                partition by opr.subscription_name_slugify
                order by opr.order_created_at asc
            ) as first_customer_id,

            -- Trial Info                  
            max(iff(trials.order_id is not null, true, false)) over (
                partition by opr.subscription_name_slugify order by trial_start_date asc
            ) as is_started_with_trial,
            first_value(trials.trial_start_date) over (
                partition by opr.subscription_name_slugify order by trial_start_date asc
            ) as trial_start_date

        from orders_with_subscriptions_without_product_plan_rate as opr
        inner join customers on opr.customer_id = customers.customer_id
        inner join
            zuora_subscription_xf
            on opr.subscription_name_slugify
            = zuora_subscription_xf.subscription_name_slugify
        left join
            zuora_rp
            on zuora_rp.subscription_id = zuora_subscription_xf.subscription_id
            and opr.product_rate_plan_id = zuora_rp.product_rate_plan_id
        inner join zuora_rpc on zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
        left join trials on opr.order_id = trials.order_id

    ),
    joined_with_customer_and_namespace_list as (

        select distinct
            rate_plan_charge_id,
            subscription_name_slugify,
            rate_plan_id,
            is_purchased_through_subscription_portal,
            current_customer_id,
            current_gitlab_namespace_id,
            first_customer_id,
            is_started_with_trial,
            trial_start_date,
            array_agg(customer_id) within group (
                order by customer_id asc
            ) as customer_id_list,
            array_agg(gitlab_namespace_id) within group (
                order by customer_id asc
            ) as gitlab_namespace_id_list
        from joined {{ dbt_utils.group_by(n=9) }}

    ),
    filtered_out_charges_with_valid_data as (

        select joined_with_customer_and_namespace_list.*
        from joined_with_customer_and_namespace_list
        left join
            orders_with_valid_charges
            on joined_with_customer_and_namespace_list.rate_plan_charge_id
            = orders_with_valid_charges.rate_plan_charge_id
        where orders_with_valid_charges.rate_plan_charge_id is null
    )

select *
from filtered_out_charges_with_valid_data
