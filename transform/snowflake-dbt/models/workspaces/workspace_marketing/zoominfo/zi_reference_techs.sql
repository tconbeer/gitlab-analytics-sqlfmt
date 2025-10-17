with source as (select * from {{ ref("zi_reference_techs_source") }})

select *
from source
