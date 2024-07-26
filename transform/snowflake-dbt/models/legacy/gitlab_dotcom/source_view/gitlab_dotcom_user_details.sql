with source as (select * from {{ ref("gitlab_dotcom_user_details_source") }})

select *
from source
