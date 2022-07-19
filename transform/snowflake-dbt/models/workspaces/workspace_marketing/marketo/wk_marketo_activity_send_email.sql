with source as (select * from {{ ref("marketo_activity_send_email_source") }})

select *
from source
