with source as (select * from {{ ref("marketo_activity_email_delivered_source") }})

select *
from source
