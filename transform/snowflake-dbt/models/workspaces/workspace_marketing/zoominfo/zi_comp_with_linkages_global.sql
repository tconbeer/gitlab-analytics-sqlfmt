with source as (select * from {{ ref("zi_comp_with_linkages_global_source") }})

select *
from source
