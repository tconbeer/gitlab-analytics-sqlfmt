with source as (select * from {{ ref("marketo_activity_sfdc_activity_source") }})

select *
from source
