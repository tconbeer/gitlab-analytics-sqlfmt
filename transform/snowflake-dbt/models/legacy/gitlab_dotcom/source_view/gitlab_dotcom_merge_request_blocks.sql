with source as (select * from {{ ref("gitlab_dotcom_merge_request_blocks_source") }})

select *
from source
