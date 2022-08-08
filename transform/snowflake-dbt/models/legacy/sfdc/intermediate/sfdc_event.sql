with source as (select * from {{ ref("sfdc_event_source") }} where is_deleted = false)

select *
from source
