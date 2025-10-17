with source as (select * from {{ ref("sheetload_gcp_active_cud_source") }})

select *
from source
