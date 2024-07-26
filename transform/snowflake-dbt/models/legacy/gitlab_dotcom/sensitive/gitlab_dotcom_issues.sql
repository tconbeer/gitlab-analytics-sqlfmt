with source as (select * from {{ ref("gitlab_dotcom_issues_source") }})

select *
from source
