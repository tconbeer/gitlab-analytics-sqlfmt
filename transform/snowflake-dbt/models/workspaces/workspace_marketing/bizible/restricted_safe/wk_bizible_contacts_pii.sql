with source as (select * from {{ ref("bizible_contacts_source_pii") }})

select *
from source
