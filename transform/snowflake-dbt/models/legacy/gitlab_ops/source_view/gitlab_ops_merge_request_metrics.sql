with source as (select * from {{ ref("gitlab_ops_merge_request_metrics_source") }})

select *
from source
