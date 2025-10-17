with source as (select * from {{ ref("gitlab_ops_projects_source") }})

select *
from source
