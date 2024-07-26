with source as (select * from {{ ref("gitlab_dotcom_deployments_source") }})

select *
from source
