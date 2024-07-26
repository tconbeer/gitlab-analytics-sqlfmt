with source as (select * from {{ ref("gitlab_dotcom_user_preferences_source") }})

select *
from source
