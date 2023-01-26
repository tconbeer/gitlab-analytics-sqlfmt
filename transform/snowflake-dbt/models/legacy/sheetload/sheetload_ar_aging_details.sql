with source as (select * from {{ ref("sheetload_ar_aging_details_source") }})

select *
from source
