with source as (select * from {{ ref("gitlab_dotcom_projects_source") }})

select *
from source
