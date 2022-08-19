with source as (select * from {{ ref("gitlab_ops_ci_stages_source") }})

select *
from source
