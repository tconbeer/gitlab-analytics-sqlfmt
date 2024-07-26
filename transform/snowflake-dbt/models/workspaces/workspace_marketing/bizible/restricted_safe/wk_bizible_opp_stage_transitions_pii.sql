with source as (select * from {{ ref("bizible_opp_stage_transitions_source_pii") }})

select *
from source
