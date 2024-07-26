with source as (select * from {{ ref("xactly_position_hist_source") }})

select *
from source
