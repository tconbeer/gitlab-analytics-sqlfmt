with source as (select * from {{ ref("sheetload_product_group_mappings_source") }})

select *
from source
