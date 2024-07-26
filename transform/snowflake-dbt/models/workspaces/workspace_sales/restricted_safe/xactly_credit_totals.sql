with source as (select * from {{ ref("xactly_credit_totals_source") }})

select *
from source
