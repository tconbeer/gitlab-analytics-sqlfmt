with source as (select * from {{ ref("demandbase_keyword_historical_rollup_source") }})

select *
from source
