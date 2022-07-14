with source as (select * from {{ ref("sheetload_planned_values_source") }})

select *
from source
