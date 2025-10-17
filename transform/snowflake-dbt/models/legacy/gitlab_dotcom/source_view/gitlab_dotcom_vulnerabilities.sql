with source as (select * from {{ ref("gitlab_dotcom_vulnerabilities_source") }})

select *
from source
