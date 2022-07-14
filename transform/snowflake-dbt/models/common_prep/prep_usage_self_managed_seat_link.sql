{{ config(tags=["mnpi_exception"]) }}

with
    seat_links as (

        select
            order_id,
            zuora_subscription_id as order_subscription_id,
            trim(zuora_subscription_id) as dim_subscription_id,
            report_date,
            active_user_count,
            license_user_count,
            max_historical_user_count,
            iff(
                row_number() over (
                    partition by order_subscription_id order by report_date desc
                )
                = 1,
                true,
                false
            ) as is_last_seat_link_report_per_subscription,
            iff(
                row_number() over (partition by order_id order by report_date desc) = 1,
                true,
                false
            ) as is_last_seat_link_report_per_order
        from {{ ref("customers_db_license_seat_links_source") }}

    ),
    customers_orders as (select * from {{ ref("customers_db_orders_source") }}),
    subscriptions as (select * from {{ ref("prep_subscription") }}),
    product_details as (

        select distinct product_rate_plan_id, dim_product_tier_id
        from {{ ref("dim_product_detail") }}
        where product_delivery_type = 'Self-Managed'

    ),
    joined as (

        select
            customers_orders.order_id as customers_db_order_id,
            seat_links.order_subscription_id,
            {{ get_keyed_nulls("subscriptions.dim_subscription_id") }}
            as dim_subscription_id,
            {{ get_keyed_nulls("subscriptions.dim_subscription_id_original") }}
            as dim_subscription_id_original,
            {{ get_keyed_nulls("subscriptions.dim_subscription_id_previous") }}
            as dim_subscription_id_previous,
            {{ get_keyed_nulls("subscriptions.dim_crm_account_id") }}
            as dim_crm_account_id,
            {{ get_keyed_nulls("subscriptions.dim_billing_account_id") }}
            as dim_billing_account_id,
            {{ get_keyed_nulls("product_details.dim_product_tier_id") }}
            as dim_product_tier_id,
            seat_links.active_user_count as active_user_count,
            seat_links.license_user_count,
            seat_links.max_historical_user_count as max_historical_user_count,
            seat_links.report_date,
            seat_links.is_last_seat_link_report_per_subscription,
            seat_links.is_last_seat_link_report_per_order,
            iff(
                ifnull(seat_links.order_subscription_id, '')
                = subscriptions.dim_subscription_id,
                true,
                false
            ) as is_subscription_in_zuora,
            iff(
                product_details.dim_product_tier_id is not null, true, false
            ) as is_rate_plan_in_zuora,
            iff(
                seat_links.active_user_count is not null, true, false
            ) as is_active_user_count_available
        from seat_links
        inner join customers_orders on seat_links.order_id = customers_orders.order_id
        left outer join
            subscriptions
            on seat_links.dim_subscription_id = subscriptions.dim_subscription_id
        left outer join
            product_details
            on customers_orders.product_rate_plan_id
            = product_details.product_rate_plan_id

    )

    {{
        dbt_audit(
            cte_ref="joined",
            created_by="@ischweickartDD",
            updated_by="@ischweickartDD",
            created_date="2021-02-02",
            updated_date="2021-02-16",
        )
    }}
