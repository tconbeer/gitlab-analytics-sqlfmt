with source as (select * from {{ ref("demandbase_account_source") }})

select *
from source
