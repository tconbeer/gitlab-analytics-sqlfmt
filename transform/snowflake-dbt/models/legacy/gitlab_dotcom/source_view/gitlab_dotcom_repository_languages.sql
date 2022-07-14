with source as (select * from {{ ref("gitlab_dotcom_repository_languages_source") }})

select *
from source
