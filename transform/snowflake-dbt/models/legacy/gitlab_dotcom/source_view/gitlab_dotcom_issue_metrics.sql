with source as (select * from {{ ref("gitlab_dotcom_issue_metrics_source") }})

select *
from source
