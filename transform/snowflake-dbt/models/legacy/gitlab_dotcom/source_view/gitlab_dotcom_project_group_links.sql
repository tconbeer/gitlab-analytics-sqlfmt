with source as (select * from {{ ref("gitlab_dotcom_project_group_links_source") }})

select *
from source
