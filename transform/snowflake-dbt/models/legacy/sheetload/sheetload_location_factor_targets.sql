with source as (select * from {{ ref("sheetload_location_factor_targets_source") }})

select *
from source
