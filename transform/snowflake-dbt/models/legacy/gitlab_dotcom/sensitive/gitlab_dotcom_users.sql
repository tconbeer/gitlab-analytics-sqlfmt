with source as (select * from {{ ref("gitlab_dotcom_users_source") }})

select *
from source
