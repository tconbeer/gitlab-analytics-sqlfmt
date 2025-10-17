with source as (select * from {{ ref("sheetload_rep_quotas_full_ps_fy2020_source") }})

select *
from source
