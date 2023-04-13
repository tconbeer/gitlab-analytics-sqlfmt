with source as (select * from {{ ref("xactly_credit_adjustment_source") }})

select *
from source
