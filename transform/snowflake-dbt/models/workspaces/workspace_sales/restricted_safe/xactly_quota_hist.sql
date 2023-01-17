with source as (select * from {{ ref("xactly_quota_hist_source") }})

select *
from source
