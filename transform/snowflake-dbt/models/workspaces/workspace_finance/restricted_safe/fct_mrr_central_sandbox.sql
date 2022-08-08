/* grain: one record per rate_plan_charge per month */
with
    mrr as (

        select
            mrr_id,
            dim_date_id,
            dim_charge_id,
            dim_product_detail_id,
            dim_subscription_id,
            dim_billing_account_id,
            dim_crm_account_id,
            subscription_status,
            mrr,
            arr,
            quantity,
            unit_of_measure
        from {{ ref("prep_recurring_charge_central_sandbox") }}
        /* This excludes Education customers (charge name EDU or OSS) with free subscriptions */
        where mrr != 0
    )

    {{
        dbt_audit(
            cte_ref="mrr",
            created_by="@michellecooper",
            updated_by="@michellecooper",
            created_date="2022-03-31",
            updated_date="2022-03-31",
        )
    }}
