with base as (select * from {{ source("snapshots", "mart_arr_snapshot") }})

select *
from base
