with source as (select * from {{ ref("sheetload_sales_capacity_kpi_source") }})

select *
from source
