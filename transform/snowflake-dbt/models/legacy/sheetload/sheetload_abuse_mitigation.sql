with source as (select * from {{ ref("sheetload_abuse_mitigation_source") }})

select *
from source
