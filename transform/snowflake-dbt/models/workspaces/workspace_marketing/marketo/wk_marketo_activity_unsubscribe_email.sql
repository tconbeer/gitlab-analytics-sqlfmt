with source as (select * from {{ ref("marketo_activity_unsubscribe_email_source") }})

select *
from source
