with source as (select * from {{ ref("sheetload_days_sales_outstanding_source") }})

select *
from source
