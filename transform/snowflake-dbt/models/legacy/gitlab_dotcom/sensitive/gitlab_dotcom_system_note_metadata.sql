with source as (select * from {{ ref("gitlab_dotcom_system_note_metadata_source") }})

select *
from source
