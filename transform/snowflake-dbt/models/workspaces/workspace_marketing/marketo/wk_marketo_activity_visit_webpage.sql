with source as (select * from {{ ref("marketo_activity_visit_webpage_source") }})

select *
from source
