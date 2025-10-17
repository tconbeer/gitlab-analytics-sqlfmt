with source as (select * from {{ ref("gitlab_dotcom_milestones_source") }})

select *
from source
