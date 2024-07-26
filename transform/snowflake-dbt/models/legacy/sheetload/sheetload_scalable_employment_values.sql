with source as (select * from {{ ref("sheetload_scalable_employment_values_source") }})

select *
from source
