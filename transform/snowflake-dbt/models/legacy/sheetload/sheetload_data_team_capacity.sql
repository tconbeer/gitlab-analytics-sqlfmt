
with source as (select * from {{ ref("sheetload_data_team_capacity_source") }})

select *
from source
