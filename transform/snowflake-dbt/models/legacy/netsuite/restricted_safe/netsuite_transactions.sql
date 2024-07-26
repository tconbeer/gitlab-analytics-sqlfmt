with source as (select * from {{ ref("netsuite_transactions_source") }})

select *
from source
