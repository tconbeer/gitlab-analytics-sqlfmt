with source as (select * from {{ ref("marketo_activity_remove_from_list_source") }})

select *
from source
