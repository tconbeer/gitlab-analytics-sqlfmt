with source as (select * from {{ ref("gitlab_dotcom_snippets_source") }})

select *
from source
