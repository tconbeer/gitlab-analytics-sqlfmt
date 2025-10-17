with source as (select * from {{ ref("sheetload_hire_plan_source") }})

select *
from source
