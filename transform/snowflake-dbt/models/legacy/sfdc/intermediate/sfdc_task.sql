with source as (select * from {{ ref("sfdc_task_source") }} where is_deleted = false)

select *
from source
