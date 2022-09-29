with source as (select * from {{ ref("gitlab_dotcom_experiments_source") }})

select *
from source
