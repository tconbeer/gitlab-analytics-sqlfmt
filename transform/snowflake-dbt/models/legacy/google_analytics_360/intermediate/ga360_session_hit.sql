with source as (select * from {{ ref("ga360_session_hit_source") }})

select *
from source
