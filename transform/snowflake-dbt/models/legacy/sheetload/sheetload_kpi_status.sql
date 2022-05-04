with source as (select * from {{ ref("sheetload_kpi_status_source") }})

select *
from source
