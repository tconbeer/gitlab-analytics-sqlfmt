with source as (select * from {{ ref("sheetload_rep_quotas_source") }})

select *
from source
