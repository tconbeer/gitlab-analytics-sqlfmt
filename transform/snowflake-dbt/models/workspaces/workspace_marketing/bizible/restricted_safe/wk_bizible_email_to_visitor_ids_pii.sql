with source as (select * from {{ ref("bizible_email_to_visitor_ids_source_pii") }})

select *
from source
