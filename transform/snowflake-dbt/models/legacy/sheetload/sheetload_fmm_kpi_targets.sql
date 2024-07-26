with source as (select * from {{ ref("sheetload_fmm_kpi_targets_source") }})

select *
from source
