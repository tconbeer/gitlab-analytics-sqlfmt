with source as (select * from {{ ref("netsuite_budget_source") }})

select *
from source
where is_fivetran_deleted = false
