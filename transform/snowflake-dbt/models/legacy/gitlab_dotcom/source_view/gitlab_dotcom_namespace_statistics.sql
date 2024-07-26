with source as (select * from {{ ref("gitlab_dotcom_namespace_statistics_source") }})

select *
from source
