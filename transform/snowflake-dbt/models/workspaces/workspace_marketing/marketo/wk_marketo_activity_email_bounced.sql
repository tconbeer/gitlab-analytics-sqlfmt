with source as (select * from {{ ref("marketo_activity_email_bounced_source") }})

select *
from source
