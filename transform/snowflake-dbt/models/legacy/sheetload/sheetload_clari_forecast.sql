with source as (select * from {{ ref("sheetload_clari_forecast_source") }})

select *
from source
