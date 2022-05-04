with source as (select * from {{ ref("demandbase_account_site_page_metrics_source") }})

select *
from source
