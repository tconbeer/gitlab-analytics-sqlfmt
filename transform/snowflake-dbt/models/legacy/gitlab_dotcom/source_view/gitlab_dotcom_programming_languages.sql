with source as (select * from {{ ref("gitlab_dotcom_programming_languages_source") }})

select *
from source
