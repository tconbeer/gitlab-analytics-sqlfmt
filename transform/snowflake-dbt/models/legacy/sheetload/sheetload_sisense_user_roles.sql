with source as (select * from {{ ref("sheetload_sisense_user_roles_source") }})

select *
from source
