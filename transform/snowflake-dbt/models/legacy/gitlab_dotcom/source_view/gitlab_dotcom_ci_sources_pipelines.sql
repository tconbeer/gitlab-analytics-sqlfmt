with source as (select * from {{ ref("gitlab_dotcom_ci_sources_pipelines_source") }})

select *
from source
