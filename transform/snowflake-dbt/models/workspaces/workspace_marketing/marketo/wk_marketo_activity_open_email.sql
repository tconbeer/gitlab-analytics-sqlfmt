with source as (select * from {{ ref("marketo_activity_open_email_source") }})

select *
from source
