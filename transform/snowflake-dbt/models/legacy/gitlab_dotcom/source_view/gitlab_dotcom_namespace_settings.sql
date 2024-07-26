with source as (select * from {{ ref("gitlab_dotcom_namespace_settings_source") }})

select *
from source
