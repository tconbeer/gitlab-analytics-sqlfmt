with source as (select * from {{ ref("gitlab_dotcom_project_auto_devops_source") }})

select *
from source
