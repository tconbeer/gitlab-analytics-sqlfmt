with source as (select * from {{ ref("sheetload_deleted_mrs_source") }})

select *
from source
