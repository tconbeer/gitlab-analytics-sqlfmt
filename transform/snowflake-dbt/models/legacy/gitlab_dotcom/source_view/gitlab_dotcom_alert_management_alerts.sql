with source as (select * from {{ ref("gitlab_dotcom_alert_management_alerts_source") }})

select *
from source
