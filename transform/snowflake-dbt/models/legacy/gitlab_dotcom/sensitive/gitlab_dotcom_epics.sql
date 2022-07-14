with source as (select * from {{ ref("gitlab_dotcom_epics_source") }})

select *
from source
