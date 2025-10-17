with source as (select * from {{ ref("sheetload_abuse_top_storage_data_source") }})

select *
from source
