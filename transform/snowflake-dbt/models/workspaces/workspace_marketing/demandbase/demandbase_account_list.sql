with source as (select * from {{ ref("demandbase_account_list_source") }})

select *
from source
