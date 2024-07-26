with source as (select * from {{ ref("xactly_plan_assignment_source") }})

select *
from source
