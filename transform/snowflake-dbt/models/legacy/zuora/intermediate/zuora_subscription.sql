-- depends_on: {{ ref('zuora_excluded_accounts') }}
with source as (select * from {{ ref("zuora_subscription_source") }})

select *
from source
where
    is_deleted = false
    and exclude_from_analysis in ('False', '')
    and account_id not in ({{ zuora_excluded_accounts() }})
