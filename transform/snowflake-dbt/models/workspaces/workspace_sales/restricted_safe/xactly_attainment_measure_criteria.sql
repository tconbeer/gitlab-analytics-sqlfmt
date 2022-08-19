with source as (select * from {{ ref("xactly_attainment_measure_criteria_source") }})

select *
from source
