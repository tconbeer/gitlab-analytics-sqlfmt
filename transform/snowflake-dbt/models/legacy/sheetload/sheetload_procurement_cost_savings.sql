with source as (select * from {{ ref("sheetload_procurement_cost_savings_source") }})

select *
from source
