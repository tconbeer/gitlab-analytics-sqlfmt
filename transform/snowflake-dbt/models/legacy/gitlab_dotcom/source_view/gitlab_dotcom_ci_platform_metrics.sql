with source as (select * from {{ ref("gitlab_dotcom_ci_platform_metrics_source") }})

select *
from source
