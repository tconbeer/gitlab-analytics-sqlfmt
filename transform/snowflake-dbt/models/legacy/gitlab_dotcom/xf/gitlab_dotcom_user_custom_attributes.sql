with source as (select * from {{ ref("gitlab_dotcom_user_custom_attributes_source") }})

select *
from source
