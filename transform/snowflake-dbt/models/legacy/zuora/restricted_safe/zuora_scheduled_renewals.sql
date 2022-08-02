with zuora_base_mrr as (select * from {{ ref("zuora_base_mrr") }})

select
    account_number,
    account_name,
    rate_plan_charge_name,
    rate_plan_charge_number,
    currency,
    effective_start_date,
    effective_end_date,
    subscription_start_date,
    exclude_from_renewal_report,
    mrr,
    mrr * 12 as arr
from zuora_base_mrr
where
    subscription_status = 'Active'
    and exclude_from_renewal_report != 'Yes'
    and date_trunc('year', effective_end_date)::date
    >= date_trunc('year', current_date)::date
