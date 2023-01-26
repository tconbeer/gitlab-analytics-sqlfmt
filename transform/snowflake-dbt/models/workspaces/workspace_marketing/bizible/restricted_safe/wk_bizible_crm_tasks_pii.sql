with source as (select * from {{ ref("bizible_crm_tasks_source_pii") }})

select *
from source
