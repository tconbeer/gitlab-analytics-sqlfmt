{% set age_of_invoice = "datediff(day, zuora_invoice.due_date, CURRENT_DATE)" %}

with
    zuora_invoice as (select * from {{ ref("zuora_invoice") }}),
    zuora_account as (select * from {{ ref("zuora_account") }}),
    open_invoices as (

        select
            zuora_account.account_name,
            case
                when {{ age_of_invoice }} < 30
                then '1: <30'
                when {{ age_of_invoice }} >= 30 and {{ age_of_invoice }} <= 60
                then '2: 30-60'
                when {{ age_of_invoice }} >= 61 and {{ age_of_invoice }} <= 90
                then '3: 61-90'
                when {{ age_of_invoice }} >= 91
                then '4: >90'
                else 'Unknown'
            end as day_range,
            listagg(zuora_invoice.invoice_number, ', ') OVER (
                partition by zuora_account.account_name
            ) as open_invoices
        from zuora_invoice
        inner join zuora_account on zuora_invoice.account_id = zuora_account.account_id
        where {{ age_of_invoice }} >= 31

    )

select account_name, day_range, max(open_invoices) as list_of_open_invoices
from open_invoices
group by 1, 2
