with source as (select * from {{ ref("gitlab_dotcom_gitlab_subscriptions_source") }})

select *
from source
