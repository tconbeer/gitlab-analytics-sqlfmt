with source as (select * from {{ ref("sfdc_campaign_source") }})

select *
from source
where is_deleted = false
