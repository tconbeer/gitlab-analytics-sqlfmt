with source as (select * from {{ ref("sheetload_linkedin_recruiter_source") }})

select *
from source
