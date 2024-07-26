with source as (select * from {{ ref("gitlab_dotcom_releases_source") }})

select *
from source
