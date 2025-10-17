with source as (select * from {{ ref("sheetload_gitlab_slack_stats_source") }})

select *
from source
