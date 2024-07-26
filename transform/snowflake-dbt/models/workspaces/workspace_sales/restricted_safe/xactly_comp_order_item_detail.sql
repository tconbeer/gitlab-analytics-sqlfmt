with source as (select * from {{ ref("xactly_comp_order_item_detail_source") }})

select *
from source
