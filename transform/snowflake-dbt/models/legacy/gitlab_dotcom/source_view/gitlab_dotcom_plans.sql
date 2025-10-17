with source as (select * from {{ ref("gitlab_dotcom_plans_source") }})

select *
from source
