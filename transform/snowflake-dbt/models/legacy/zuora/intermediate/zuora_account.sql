-- depends_on: {{ ref('zuora_excluded_accounts') }}
with source as (select * from {{ ref("zuora_account_source") }})

select *
from source
where is_deleted = false and account_id not in ({{ zuora_excluded_accounts() }})
