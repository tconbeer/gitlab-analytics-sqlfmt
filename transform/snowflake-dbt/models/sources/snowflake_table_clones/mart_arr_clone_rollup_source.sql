with source as (select * from {{ source("full_table_clones", "mart_arr_rollup") }})

select *
from source
