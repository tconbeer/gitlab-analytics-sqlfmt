{{ config({"schema": "restricted_safe_legacy"}) }}

with
    zuora_invoice as (select * from {{ ref("zuora_invoice_source") }}),
    zuora_invoice_item as (select * from {{ ref("zuora_invoice_item_source") }}),
    invoice_data as (

        select
            zuora_invoice_item.rate_plan_charge_id as charge_id,
            zuora_invoice_item.sku as sku,
            min(zuora_invoice_item.service_start_date::date) as service_start_date,
            max(zuora_invoice_item.service_end_date::date) as service_end_date,
            sum(zuora_invoice_item.charge_amount) as charge_amount_sum,
            sum(zuora_invoice_item.tax_amount) as tax_amount_sum
        from zuora_invoice_item
        inner join
            zuora_invoice on zuora_invoice_item.invoice_id = zuora_invoice.invoice_id
        where
            zuora_invoice.is_deleted = false
            and zuora_invoice_item.is_deleted = false
            and zuora_invoice.status = 'Posted'
        group by 1, 2
    )

select *
from invoice_data
