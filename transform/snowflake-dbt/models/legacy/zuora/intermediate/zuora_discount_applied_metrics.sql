with source as (select * from {{ ref("zuora_discount_applied_metrics_source") }})

select *
from source
