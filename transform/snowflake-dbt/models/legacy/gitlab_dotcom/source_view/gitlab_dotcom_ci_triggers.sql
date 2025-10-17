with source as (select * from {{ ref("gitlab_dotcom_ci_triggers_source") }})

select *
from source
