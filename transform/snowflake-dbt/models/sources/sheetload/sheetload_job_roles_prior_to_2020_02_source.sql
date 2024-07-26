with
    source as (select * from {{ source("sheetload", "job_roles_prior_to_2020_02") }}),
    final as (select job_title, job_role from source)

select *
from final
