with source as (select * from {{ ref("marketo_activity_type_source") }})

select *
from source
