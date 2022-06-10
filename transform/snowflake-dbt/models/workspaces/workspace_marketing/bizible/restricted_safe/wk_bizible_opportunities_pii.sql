with source as (select * from {{ ref("bizible_opportunities_source_pii") }})

select *
from source
