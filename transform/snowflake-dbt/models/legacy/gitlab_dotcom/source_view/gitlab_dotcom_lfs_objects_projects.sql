with source as (select * from {{ ref("gitlab_dotcom_lfs_objects_projects_source") }})

select *
from source
