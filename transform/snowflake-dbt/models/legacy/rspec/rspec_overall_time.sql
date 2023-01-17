with source as (select * from {{ ref("rspec_overall_time_source") }})

select *
from source
