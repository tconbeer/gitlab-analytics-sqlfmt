{{
    simple_cte(
        [
            ("rate_plans", "zuora_rate_plan_source"),
            ("product_details", "dim_product_detail"),
        ]
    )
}}

,
subscriptions as (

    select *
    from {{ ref("zuora_subscription_source") }}
    where is_deleted = false and exclude_from_analysis in ('False', '')

),
joined as (

    select distinct
        subscriptions.subscription_id as dim_subscription_id,
        subscriptions.original_id as dim_subscription_id_original,
        subscriptions.account_id as dim_billing_account_id,
        product_details.dim_product_tier_id,
        product_details.dim_product_detail_id,
        product_details.product_id,
        product_details.product_rate_plan_id,
        rate_plans.rate_plan_id,
        subscriptions.subscription_name,
        subscriptions.subscription_name_slugify,
        subscriptions.subscription_start_date,
        subscriptions.subscription_end_date,
        subscriptions.subscription_status,
        product_details.product_rate_plan_charge_name,
        product_details.product_delivery_type
    from subscriptions
    left join rate_plans on subscriptions.subscription_id = rate_plans.subscription_id
    left join
        product_details
        on rate_plans.product_rate_plan_id = product_details.product_rate_plan_id
    where rate_plans.amendement_type != 'RemoveProduct'

)

{{
    dbt_audit(
        cte_ref="joined",
        created_by="@ischweickartDD",
        updated_by="@ischweickartDD",
        created_date="2021-02-08",
        updated_date="2021-07-23",
    )
}}
