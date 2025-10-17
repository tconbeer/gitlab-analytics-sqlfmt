with source as (select * from {{ ref("gitlab_dotcom_group_group_links_source") }})

select *
from source
