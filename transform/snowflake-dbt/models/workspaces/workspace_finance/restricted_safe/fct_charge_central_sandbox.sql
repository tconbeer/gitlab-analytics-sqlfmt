with
    prep_charge as (select * from {{ ref("prep_charge") }}),
    prep_amendment as (select * from {{ ref("prep_amendment") }}),
    fct_charge as (

        select
            -- Surrogate Key
            prep_charge.dim_charge_id,

            -- Natural Key
            prep_charge.subscription_name,
            prep_charge.subscription_version,
            prep_charge.rate_plan_charge_number,
            prep_charge.rate_plan_charge_version,
            prep_charge.rate_plan_charge_segment,

            -- Common Dimension Keys
            prep_charge.dim_product_detail_id,
            {{ get_keyed_nulls("prep_amendment.dim_amendment_id") }}
            as dim_amendment_id_charge,
            prep_charge.dim_subscription_id,
            prep_charge.dim_billing_account_id,
            prep_charge.dim_crm_account_id,
            prep_charge.dim_parent_crm_account_id,
            prep_charge.effective_start_date_id,
            prep_charge.effective_end_date_id,

            -- Additive Fields
            prep_charge.mrr,
            prep_charge.previous_mrr,
            prep_charge.delta_mrr,
            prep_charge.arr,
            prep_charge.previous_arr,
            prep_charge.delta_arr,
            prep_charge.quantity,
            prep_charge.previous_quantity,
            prep_charge.delta_quantity,
            prep_charge.delta_tcv,
            prep_charge.estimated_total_future_billings,

            prep_charge.unit_of_measure,
            prep_charge.charge_type,
            prep_charge.effective_start_month,
            prep_charge.effective_end_month

        from prep_charge
        left join
            prep_amendment
            on prep_charge.dim_amendment_id_charge = prep_amendment.dim_amendment_id
        order by
            prep_charge.dim_parent_crm_account_id,
            prep_charge.dim_crm_account_id,
            subscription_name,
            subscription_version,
            rate_plan_charge_number,
            rate_plan_charge_version,
            rate_plan_charge_segment

    )

    {{
        dbt_audit(
            cte_ref="fct_charge",
            created_by="@michellecooper",
            updated_by="@michellecooper",
            created_date="2022-04-13",
            updated_date="2022-04-13",
        )
    }}
