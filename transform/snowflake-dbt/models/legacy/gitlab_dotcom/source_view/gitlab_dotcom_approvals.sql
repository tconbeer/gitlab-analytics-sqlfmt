with source as (select * from {{ ref("gitlab_dotcom_approvals_source") }})

select *
from source
