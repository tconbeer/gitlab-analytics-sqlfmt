with source as (select * from {{ ref("zuora_invoice_item_source") }})

select *
from source
where is_deleted = false
