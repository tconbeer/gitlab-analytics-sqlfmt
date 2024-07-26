with source as (select * from {{ ref("sheetload_headcount_source") }})

select *
from source
