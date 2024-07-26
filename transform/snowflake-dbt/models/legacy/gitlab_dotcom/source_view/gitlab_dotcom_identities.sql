with source as (select * from {{ ref("gitlab_dotcom_identities_source") }})

select *
from source
