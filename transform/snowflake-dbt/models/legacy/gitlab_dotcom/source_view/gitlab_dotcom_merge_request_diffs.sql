with source as (select * from {{ ref("gitlab_dotcom_merge_request_diffs_source") }})

select *
from source
