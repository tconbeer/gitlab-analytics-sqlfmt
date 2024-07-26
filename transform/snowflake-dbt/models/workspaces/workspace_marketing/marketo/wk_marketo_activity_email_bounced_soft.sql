with source as (select * from {{ ref("marketo_activity_email_bounced_soft_source") }})

select *
from source
