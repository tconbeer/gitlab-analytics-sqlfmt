with source as (select * from {{ ref("gitlab_dotcom_ci_pipeline_schedules_source") }})

select *
from source
