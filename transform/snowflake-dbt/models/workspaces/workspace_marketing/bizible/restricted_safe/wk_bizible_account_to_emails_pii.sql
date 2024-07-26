with source as (select * from {{ ref("bizible_account_to_emails_source_pii") }})

select *
from source
