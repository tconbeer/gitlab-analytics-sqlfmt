with source as (select * from {{ ref("marketo_activity_change_score_source") }})

select *
from source
