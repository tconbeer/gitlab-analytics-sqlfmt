with source as (select * from {{ ref("xactly_pos_hierarchy_type_hist_source") }})

select *
from source
