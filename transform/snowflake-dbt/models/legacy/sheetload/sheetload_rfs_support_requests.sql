with source as (select * from {{ ref("sheetload_rfs_support_requests_source") }})

select *
from source
