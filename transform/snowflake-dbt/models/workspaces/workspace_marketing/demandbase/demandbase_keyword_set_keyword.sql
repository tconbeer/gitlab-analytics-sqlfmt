with source as (select * from {{ ref("demandbase_keyword_set_keyword_source") }})

select *
from source
