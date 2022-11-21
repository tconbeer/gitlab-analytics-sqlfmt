with source as (select * from {{ ref("gitlab_dotcom_ci_pipelines_source") }})

select *
from source
