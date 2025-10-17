with source as (select * from {{ ref("sheetload_hire_forecast_source") }})

select *
from source
