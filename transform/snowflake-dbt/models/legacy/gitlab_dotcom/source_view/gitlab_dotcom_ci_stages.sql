with source as (select * from {{ ref("gitlab_dotcom_ci_stages_source") }})

select *
from source
