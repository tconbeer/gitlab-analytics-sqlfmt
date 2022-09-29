with source as (select * from {{ ref("gitlab_dotcom_resource_label_events_source") }})

select *
from source
