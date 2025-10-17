with source as (select * from {{ ref("sheetload_sdr_attainment_to_goal_source") }})

select *
from source
