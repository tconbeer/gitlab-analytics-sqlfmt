with source as (select * from {{ ref("xactly_credit_held_source") }})

select *
from source
