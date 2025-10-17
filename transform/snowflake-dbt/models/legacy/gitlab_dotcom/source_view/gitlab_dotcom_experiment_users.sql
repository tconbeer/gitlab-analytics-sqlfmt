with source as (select * from {{ ref("gitlab_dotcom_experiment_users_source") }})

select *
from source
