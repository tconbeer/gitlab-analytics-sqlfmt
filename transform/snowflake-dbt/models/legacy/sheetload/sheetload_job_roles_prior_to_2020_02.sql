with source as (select * from {{ ref("sheetload_job_roles_prior_to_2020_02_source") }})

select *
from source
