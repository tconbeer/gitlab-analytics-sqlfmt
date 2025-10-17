-- this can't be an incremental model because of the day_range calculation
with
    zuora_invoice_base as (

        select *
        from {{ ref("zuora_invoice") }}
        where date_trunc(day, due_date)::date < current_date

    ),
    zuora_account as (select * from {{ ref("zuora_account") }}),
    zuora_contact as (select * from {{ ref("zuora_contact") }}),
    zuora_invoice as (

        select *, abs(datediff('day', current_date, due_date)) as days_overdue
        from zuora_invoice_base

    )

select
    zuora_account.sfdc_entity as entity,
    zuora_invoice.invoice_id,
    coalesce(zuora_contact_bill.work_email, zuora_contact_sold.work_email) as email,
    coalesce(zuora_contact_sold.first_name, zuora_contact_bill.first_name) as owner,
    zuora_account.account_name,
    zuora_account.account_number,
    zuora_account.currency,

    case
        when days_overdue < 30
        then '1: <30'
        when days_overdue >= 30 and days_overdue <= 60
        then '2: 30-60'
        when days_overdue >= 61 and days_overdue <= 90
        then '3: 61-90'
        when days_overdue >= 91
        then '4: >90'
        else 'Unknown'
    end as range_since_due_date,

    coalesce(zuora_invoice.balance, 0) as balance,

    zuora_invoice.status as invoice_status,
    zuora_invoice.invoice_number as invoice,
    zuora_invoice.due_date as due_date

from zuora_invoice
inner join zuora_account on zuora_invoice.account_id = zuora_account.account_id
left join
    zuora_contact as zuora_contact_bill
    on zuora_contact_bill.contact_id = zuora_account.bill_to_contact_id  -- I don't really love this method, but it works. 
left join
    zuora_contact as zuora_contact_sold
    on zuora_contact_sold.contact_id = zuora_account.sold_to_contact_id
