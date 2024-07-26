with source as (select * from {{ source("sheetload", "product_maturity_historical") }})

select *
from source
