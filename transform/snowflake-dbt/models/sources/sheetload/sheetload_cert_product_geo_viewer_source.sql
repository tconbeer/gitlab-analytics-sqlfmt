with source as (select * from {{ source("sheetload", "cert_product_geo_viewer") }})

select *
from source
