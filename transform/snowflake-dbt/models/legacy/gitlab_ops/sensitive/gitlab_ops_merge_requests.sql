with source as (select * from {{ ref("gitlab_ops_merge_requests_source") }})

select *
from source
