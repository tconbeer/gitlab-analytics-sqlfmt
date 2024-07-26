with source as (select * from {{ ref("gitlab_dotcom_ci_runners_source") }})

select *
from source
