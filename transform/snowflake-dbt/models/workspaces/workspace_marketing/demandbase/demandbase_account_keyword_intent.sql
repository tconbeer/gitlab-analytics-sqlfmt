with source as (select * from {{ ref("demandbase_account_keyword_intent_source") }})

select *
from source
