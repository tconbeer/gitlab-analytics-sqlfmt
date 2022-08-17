with source as (select * from {{ ref("gitlab_ops_ci_pipelines_source") }})

select *
from source
