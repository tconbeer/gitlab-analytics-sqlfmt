with source as (select * from {{ ref("gitlab_dotcom_boards_source") }})

select *
from source
