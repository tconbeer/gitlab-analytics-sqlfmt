with source as (select * from {{ ref("sheetload_product_maturity_historical_source") }})

select *
from source
