with source as (select * from {{ ref("gitlab_dotcom_dast_profiles_source") }})

select *
from source
