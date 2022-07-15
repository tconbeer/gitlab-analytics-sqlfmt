with source as (select * from {{ ref("gitlab_dotcom_terraform_states_source") }})

select *
from source
