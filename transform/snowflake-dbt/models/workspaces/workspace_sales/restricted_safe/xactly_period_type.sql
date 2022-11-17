with source as (select * from {{ ref("xactly_period_type_source") }})

select *
from source
