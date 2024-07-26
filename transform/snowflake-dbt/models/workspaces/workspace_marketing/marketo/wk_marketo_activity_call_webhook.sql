with source as (select * from {{ ref("marketo_activity_call_webhook_source") }})

select *
from source
