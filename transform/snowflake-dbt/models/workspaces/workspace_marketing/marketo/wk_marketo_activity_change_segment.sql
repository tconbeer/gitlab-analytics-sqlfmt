with source as (select * from {{ ref("marketo_activity_change_segment_source") }})

select *
from source
