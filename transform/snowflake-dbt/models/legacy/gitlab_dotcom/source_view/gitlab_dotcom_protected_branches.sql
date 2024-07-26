with source as (select * from {{ ref("gitlab_dotcom_protected_branches_source") }})

select *
from source
