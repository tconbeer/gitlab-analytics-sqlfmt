with source as (select * from {{ ref("netsuite_departments_source") }})

select *
from source
