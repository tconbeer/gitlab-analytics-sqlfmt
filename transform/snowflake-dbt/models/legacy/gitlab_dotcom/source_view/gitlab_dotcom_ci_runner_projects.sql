with source as (select * from {{ ref("gitlab_dotcom_ci_runner_projects_source") }})

select *
from source
