with source as (select * from {{ ref("bizible_crm_events_source_pii") }})

select *
from source
