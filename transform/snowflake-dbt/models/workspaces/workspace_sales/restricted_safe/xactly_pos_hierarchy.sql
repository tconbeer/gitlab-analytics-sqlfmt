with source as (select * from {{ ref("xactly_pos_hierarchy_source") }})

select *
from source
