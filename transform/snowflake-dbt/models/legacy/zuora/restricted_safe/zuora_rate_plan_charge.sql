with source as (select * from {{ ref("zuora_rate_plan_charge_source") }})

select *
from source
where is_deleted = false and account_id not in ({{ zuora_excluded_accounts() }})
