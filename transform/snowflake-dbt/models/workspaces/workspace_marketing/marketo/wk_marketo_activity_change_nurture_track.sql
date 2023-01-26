with source as (select * from {{ ref("marketo_activity_change_nurture_track_source") }})

select *
from source
