with source as (select * from {{ ref("demandbase_account_scores_source") }})

select *
from source
