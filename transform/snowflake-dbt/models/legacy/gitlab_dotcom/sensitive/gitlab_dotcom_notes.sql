with source as (select * from {{ ref("gitlab_dotcom_notes_source") }})

select *
from source
