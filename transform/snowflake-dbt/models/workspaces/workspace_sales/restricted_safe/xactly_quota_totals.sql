with source as (select * from {{ ref("xactly_quota_totals_source") }})

select *
from source
