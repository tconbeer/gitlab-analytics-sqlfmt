with source as (select * from {{ ref("xactly_attainment_measure_source") }})

select *
from source
