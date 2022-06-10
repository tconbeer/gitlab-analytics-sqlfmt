with source as (select * from {{ ref("sheetload_yc_companies_source") }})

select *
from source
