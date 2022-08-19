with source as (select * from {{ ref("gitlab_dotcom_onboarding_progresses_source") }})
select *
from source
