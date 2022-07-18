with source as (select * from {{ ref("gitlab_dotcom_project_statistics_source") }})

select *
from source
