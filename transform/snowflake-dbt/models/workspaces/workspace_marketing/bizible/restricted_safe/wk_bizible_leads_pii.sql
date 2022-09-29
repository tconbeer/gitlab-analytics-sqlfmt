with source as (select * from {{ ref("bizible_leads_source_pii") }})

select *
from source
