with source as (select * from {{ ref("bizible_attribution_touchpoints_source_pii") }})

select *
from source
