with source as (select * from {{ ref("marketo_activity_send_alert_source") }})

select *
from source
