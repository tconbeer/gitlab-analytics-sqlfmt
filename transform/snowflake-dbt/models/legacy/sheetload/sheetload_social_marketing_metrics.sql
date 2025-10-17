with source as (select * from {{ ref("sheetload_social_marketing_metrics_source") }})

select *
from source
