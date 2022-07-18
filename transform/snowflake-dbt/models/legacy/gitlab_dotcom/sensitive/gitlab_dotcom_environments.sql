with source as (select * from {{ ref("gitlab_dotcom_environments_source") }})

select *
from source
