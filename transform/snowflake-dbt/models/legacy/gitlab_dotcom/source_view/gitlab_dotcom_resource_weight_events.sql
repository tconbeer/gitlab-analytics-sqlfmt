with source as (select * from {{ ref("gitlab_dotcom_resource_weight_events_source") }})

select *
from source
