with source as (select * from {{ ref("gitlab_dotcom_issue_assignees_source") }})

select *
from source
