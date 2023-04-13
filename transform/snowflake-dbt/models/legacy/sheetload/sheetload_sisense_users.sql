with source as (select * from {{ ref("sheetload_sisense_users_source") }})

select *
from source
