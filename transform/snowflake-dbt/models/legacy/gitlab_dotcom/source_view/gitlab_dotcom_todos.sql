with source as (select * from {{ ref("gitlab_dotcom_todos_source") }})

select *
from source
