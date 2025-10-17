with source as (select * from {{ ref("gitlab_dotcom_services_source") }})

select *
from source
