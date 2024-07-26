with source as (select * from {{ ref("gitlab_dotcom_security_scans_source") }})

select *
from source
