with source as (select * from {{ ref("bizible_touchpoints_source_pii") }})

select *
from source
