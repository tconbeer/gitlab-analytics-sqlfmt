with source as (select * from {{ ref("gitlab_dotcom_push_rules_source") }})

select *
from source
