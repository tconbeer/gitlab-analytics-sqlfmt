with
    zuora_account as (select * from {{ ref("zuora_account") }}),
    zuora_invoice as (select * from {{ ref("zuora_invoice") }}),
    zuora_invoice_item as (select * from {{ ref("zuora_invoice_item") }}),
    zuora_product as (

        select * from {{ ref("zuora_product_source") }} where is_deleted = false

    ),
    zuora_rate_plan as (select * from {{ ref("zuora_rate_plan") }}),
    zuora_rate_plan_charge as (select * from {{ ref("zuora_rate_plan_charge") }}),
    zuora_subscription as (select * from {{ ref("zuora_subscription") }}),
    base_charges as (

        select
            zuora_account.account_id,
            zuora_account.crm_id,
            zuora_subscription.subscription_id,
            zuora_subscription.subscription_name_slugify,
            zuora_subscription.subscription_status,
            zuora_subscription.version as subscription_version,
            zuora_rate_plan.rate_plan_name,
            zuora_rate_plan_charge.rate_plan_charge_id,
            zuora_rate_plan_charge.product_rate_plan_charge_id,
            zuora_rate_plan_charge.rate_plan_charge_number,
            zuora_rate_plan_charge.rate_plan_charge_name,
            zuora_rate_plan_charge.segment as rate_plan_charge_segment,
            zuora_rate_plan_charge.version as rate_plan_charge_version,
            zuora_rate_plan_charge.effective_start_date::date as effective_start_date,
            zuora_rate_plan_charge.effective_end_date::date as effective_end_date,
            zuora_rate_plan_charge.unit_of_measure,
            zuora_rate_plan_charge.quantity,
            zuora_rate_plan_charge.mrr,
            zuora_rate_plan_charge.delta_tcv,
            zuora_rate_plan_charge.charge_type,
            zuora_product.product_name
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
            zuora_product
            on zuora_rate_plan_charge.product_id = zuora_product.product_id

    ),
    invoice_charges as (

        select
            zuora_invoice.invoice_number,
            zuora_invoice_item.invoice_item_id,
            zuora_invoice.account_id as invoice_account_id,
            zuora_invoice.invoice_date::date as invoice_date,
            zuora_invoice_item.service_start_date::date as service_start_date,
            zuora_invoice_item.service_end_date::date as service_end_date,
            zuora_invoice.amount_without_tax as invoice_amount_without_tax,
            zuora_invoice_item.charge_amount as invoice_item_charge_amount,
            zuora_invoice_item.unit_price as invoice_item_unit_price,
            zuora_invoice_item.rate_plan_charge_id
        from zuora_invoice_item
        inner join
            zuora_invoice on zuora_invoice_item.invoice_id = zuora_invoice.invoice_id
        where zuora_invoice.status = 'Posted'

    ),
    final as (

        select
            base_charges.*,
            row_number() OVER (
                partition by rate_plan_charge_number
                order by
                    rate_plan_charge_segment,
                    rate_plan_charge_version,
                    service_start_date
            ) as segment_version_order,
            iff(
                row_number() OVER (
                    partition by rate_plan_charge_number, rate_plan_charge_segment
                    order by rate_plan_charge_version desc, service_start_date desc
                )
                = 1,
                true,
                false
            ) as is_last_segment_version,
            invoice_account_id,
            invoice_number,
            invoice_item_id,
            invoice_date,
            service_start_date,
            service_end_date,
            invoice_amount_without_tax,
            invoice_item_charge_amount,
            invoice_item_unit_price
        from base_charges
        inner join
            invoice_charges
            on base_charges.rate_plan_charge_id = invoice_charges.rate_plan_charge_id

    )

select *
from final
