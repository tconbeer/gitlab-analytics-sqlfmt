with source as (select * from {{ ref("sheetload_sdr_count_snapshot_source") }})

select *
from source
