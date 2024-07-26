with source as (select * from {{ ref("gitlab_dotcom_requirements_source") }})

select *
from source
