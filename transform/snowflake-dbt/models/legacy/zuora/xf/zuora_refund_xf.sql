with
    zuora_refund_base as (select * from {{ ref("zuora_refund") }}),
    zuora_account as (select * from {{ ref("zuora_account") }})

select
    zr.*,
    date_trunc('month', zr.refund_date)::date as refund_month,
    za.sfdc_entity,
    za.account_name,
    za.account_number,
    za.currency

from zuora_refund_base zr
left join zuora_account za on zr.account_id = za.account_id
