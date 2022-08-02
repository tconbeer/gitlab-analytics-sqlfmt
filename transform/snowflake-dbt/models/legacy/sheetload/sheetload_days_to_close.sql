with source as (select * from {{ ref("sheetload_days_to_close_source") }})

select *
from source
