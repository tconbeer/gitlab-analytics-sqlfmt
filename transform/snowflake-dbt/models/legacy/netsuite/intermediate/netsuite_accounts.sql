with source as (select * from {{ ref("netsuite_accounts_source") }})

select *
from source
