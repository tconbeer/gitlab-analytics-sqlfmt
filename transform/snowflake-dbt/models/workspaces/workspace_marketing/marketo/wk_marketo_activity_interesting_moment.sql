with source as (select * from {{ ref("marketo_activity_interesting_moment_source") }})

select *
from source
