with source as (select * from {{ ref("xactly_comp_order_item_source") }})

select *
from source
