with
    source as (select * from {{ ref("marketo_activity_sfdc_activity_updated_source") }})

select *
from source
