with source as (select * from {{ ref("gitlab_dotcom_packages_packages_source") }})

select *
from source
