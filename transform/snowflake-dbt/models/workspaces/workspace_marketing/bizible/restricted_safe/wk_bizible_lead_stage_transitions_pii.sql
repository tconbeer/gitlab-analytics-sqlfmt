with source as (select * from {{ ref("bizible_lead_stage_transitions_source_pii") }})

select *
from source
