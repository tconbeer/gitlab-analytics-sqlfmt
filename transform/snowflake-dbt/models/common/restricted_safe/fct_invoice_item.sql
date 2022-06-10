with
    map_merged_crm_account as (select * from {{ ref("map_merged_crm_account") }}),
    zuora_account as (

        select *
        from {{ ref("zuora_account_source") }}
        where lower(batch) != 'batch20' and is_deleted = false

    ),
    zuora_invoice as (

        select * from {{ ref("zuora_invoice_source") }} where is_deleted = false

    ),
    zuora_invoice_item as (

        select * from {{ ref("zuora_invoice_item_source") }} where is_deleted = false

    ),
    zuora_rate_plan as (

        select * from {{ ref("zuora_rate_plan_source") }} where is_deleted = false

    ),
    zuora_rate_plan_charge as (

        select *
        from {{ ref("zuora_rate_plan_charge_source") }}
        where is_deleted = false

    ),
    zuora_subscription as (

        select *
        from {{ ref("zuora_subscription_source") }}
        where is_deleted = false and exclude_from_analysis in ('False', '')

    ),
    zuora_revenue_bill as (

        select * from {{ ref("zuora_revenue_revenue_contract_bill_source") }}

    ),
    base_charges as (

        select
            zuora_account.account_id as billing_account_id_subscription,
            map_merged_crm_account.dim_crm_account_id as crm_account_id_subscription,
            zuora_subscription.subscription_id,
            zuora_rate_plan_charge.rate_plan_charge_id as charge_id,
            zuora_rate_plan_charge.rate_plan_charge_number,
            zuora_rate_plan_charge.segment as rate_plan_charge_segment,
            zuora_rate_plan_charge.version as rate_plan_charge_version,
            zuora_rate_plan_charge.mrr,
            zuora_rate_plan_charge.mrr * 12 as arr,
            zuora_rate_plan_charge.quantity,
            date_trunc(
                'month', zuora_rate_plan_charge.effective_start_date::date
            ) as effective_start_month,
            date_trunc(
                'month', zuora_rate_plan_charge.effective_end_date::date
            ) as effective_end_month
        from zuora_account
        inner join
            zuora_subscription
            on zuora_account.account_id = zuora_subscription.account_id
        inner join
            zuora_rate_plan
            on zuora_subscription.subscription_id = zuora_rate_plan.subscription_id
        inner join
            zuora_rate_plan_charge
            on zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
        left join
            map_merged_crm_account
            on zuora_account.crm_id = map_merged_crm_account.sfdc_account_id

    ),
    invoice_charges as (

        select
            zuora_invoice_item.invoice_item_id as invoice_item_id,
            zuora_invoice.invoice_id as invoice_id,
            zuora_invoice.invoice_number,
            zuora_invoice.invoice_date::date as invoice_date,
            zuora_invoice_item.service_start_date::date as service_start_date,
            zuora_invoice_item.service_end_date::date as service_end_date,
            zuora_invoice.account_id as billing_account_id_invoice,
            map_merged_crm_account.dim_crm_account_id as crm_account_id_invoice,
            zuora_invoice_item.rate_plan_charge_id as charge_id,
            zuora_invoice_item.product_rate_plan_charge_id as product_details_id,
            zuora_invoice_item.sku as sku,
            zuora_invoice_item.tax_amount as tax_amount_sum,
            zuora_invoice.amount_without_tax as invoice_amount_without_tax,
            zuora_invoice_item.charge_amount as invoice_item_charge_amount,
            zuora_invoice_item.unit_price as invoice_item_unit_price
        from zuora_invoice_item
        inner join
            zuora_invoice on zuora_invoice_item.invoice_id = zuora_invoice.invoice_id
        inner join zuora_account on zuora_invoice.account_id = zuora_account.account_id
        left join
            map_merged_crm_account
            on zuora_account.crm_id = map_merged_crm_account.sfdc_account_id
        where zuora_invoice.status = 'Posted'

    ),
    final as (

        select
            invoice_charges.invoice_item_id,
            invoice_charges.invoice_id as dim_invoice_id,
            base_charges.billing_account_id_subscription
            as dim_billing_account_id_subscription,
            base_charges.crm_account_id_subscription as dim_crm_account_id_subscription,
            invoice_charges.billing_account_id_invoice
            as dim_billing_account_id_invoice,
            invoice_charges.crm_account_id_invoice as dim_crm_account_id_invoice,
            base_charges.subscription_id as dim_subscription_id,
            zuora_revenue_bill.revenue_contract_line_id as dim_revenue_contract_line_id,
            invoice_charges.charge_id,
            invoice_charges.product_details_id as dim_product_detail_id,
            invoice_charges.invoice_number,
            invoice_charges.invoice_date,
            invoice_charges.service_start_date,
            invoice_charges.service_end_date,
            base_charges.effective_start_month,
            base_charges.effective_end_month,
            base_charges.quantity,
            base_charges.mrr,
            base_charges.arr,
            invoice_charges.invoice_item_charge_amount,
            invoice_charges.invoice_item_unit_price,
            invoice_charges.invoice_amount_without_tax,
            invoice_charges.tax_amount_sum,
            iff(
                row_number() over (
                    partition by rate_plan_charge_number, rate_plan_charge_segment
                    order by rate_plan_charge_version desc, service_start_date desc
                ) = 1,
                true,
                false
            ) as is_last_segment_version
        from base_charges
        inner join invoice_charges on base_charges.charge_id = invoice_charges.charge_id
        left join
            zuora_revenue_bill
            on invoice_charges.invoice_item_id = zuora_revenue_bill.invoice_item_id

    )

    {{
        dbt_audit(
            cte_ref="final",
            created_by="@mcooperDD",
            updated_by="@michellecooper",
            created_date="2021-01-15",
            updated_date="2021-06-21",
        )
    }}
