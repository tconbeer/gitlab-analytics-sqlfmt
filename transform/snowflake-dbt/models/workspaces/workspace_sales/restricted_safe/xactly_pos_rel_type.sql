with source as (select * from {{ ref("xactly_pos_rel_type_source") }})

select *
from source
