with
    zuora_base_mrr as (select * from {{ ref("zuora_base_mrr") }})


    ,
    unioned_charges as (

        {{
            dbt_utils.union_relations(
                relations=[
                    ref("customers_db_orders_with_valid_charges"),
                    ref("customers_db_orders_with_incomplete_charges"),
                ],
            )
        }}

    )

    ,
    joined_with_base_mrr as (

        select
            unioned_charges.rate_plan_charge_id,
            unioned_charges.subscription_name_slugify,
            unioned_charges.rate_plan_id,
            unioned_charges.is_purchased_through_subscription_portal,
            unioned_charges.current_customer_id,
            unioned_charges.current_gitlab_namespace_id,
            unioned_charges.first_customer_id,
            unioned_charges.is_started_with_trial,
            unioned_charges.trial_start_date,

            -- Subscription metadata
            zuora_base_mrr.lineage,
            zuora_base_mrr.oldest_subscription_in_cohort,
            zuora_base_mrr.subscription_start_date,
            zuora_base_mrr.subscription_status,

            zuora_base_mrr.effective_start_date,
            zuora_base_mrr.effective_end_date,
            zuora_base_mrr.subscription_version_term_start_date,
            zuora_base_mrr.subscription_version_term_end_date,
            zuora_base_mrr.month_interval,

            -- Product Category Info
            zuora_base_mrr.delivery,
            zuora_base_mrr.product_category,
            zuora_base_mrr.quantity,
            zuora_base_mrr.unit_of_measure,

            -- Financial Info
            zuora_base_mrr.mrr,
            zuora_base_mrr.tcv
        from unioned_charges
        inner join
            zuora_base_mrr
            on unioned_charges.rate_plan_charge_id = zuora_base_mrr.rate_plan_charge_id

    )

select *
from joined_with_base_mrr
