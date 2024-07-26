with source as (select * from {{ ref("sheetload_sdr_adaptive_data_source") }})

select *
from source
