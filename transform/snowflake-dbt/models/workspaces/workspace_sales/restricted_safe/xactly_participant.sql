with source as (select * from {{ ref("xactly_participant_source") }})

select *
from source
