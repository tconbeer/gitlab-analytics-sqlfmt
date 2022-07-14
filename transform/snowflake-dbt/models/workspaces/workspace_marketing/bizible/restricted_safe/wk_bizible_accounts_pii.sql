with source as (select * from {{ ref("bizible_accounts_source_pii") }})

select *
from source
