with source as (select * from {{ ref("xactly_credit_type_source") }})

select *
from source
