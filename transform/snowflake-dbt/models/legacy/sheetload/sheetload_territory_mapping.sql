with source as (select * from {{ ref("sheetload_territory_mapping_source") }})

select *
from source
