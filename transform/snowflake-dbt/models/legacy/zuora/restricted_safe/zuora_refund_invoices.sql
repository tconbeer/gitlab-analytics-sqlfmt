with
    invoices as (select * from {{ ref("zuora_invoice") }}),
    /* Self join invoices before and after the invoice date to get a sum of other transactions paid. */
    zuora_account as (select * from {{ ref("zuora_account") }}),
    joined as (

        select distinct
            invoices.invoice_id,
            invoices.amount as invoice_amount,
            invoices.account_id,
            invoices.invoice_date,
            zuora_account.crm_id,
            zuora_account.sfdc_entity,
            zuora_account.account_name,
            zuora_account.account_number,
            zuora_account.currency
        from invoices
        left join zuora_account on invoices.account_id = zuora_account.account_id
        left join  -- 60 day window before and after the invoice date.
            invoices as before_and_after
            on invoices.account_id = before_and_after.account_id
            and before_and_after.invoice_date between dateadd(
                'days', -60, invoices.invoice_date
            ) and dateadd('days', 60, invoices.invoice_date)
            {{ dbt_utils.group_by(9) }}
        -- To count as a refund, the customer must up even ($0) or better (<$0)
        having sum(coalesce(before_and_after.amount, 0)) <= 0

    )

select *
from joined
where invoice_amount < 0  -- Only include the rows that are actually negative
order by invoice_date, invoice_amount
