with source as (select * from {{ ref("bizible_facts_source_pii") }})

select *
from source
