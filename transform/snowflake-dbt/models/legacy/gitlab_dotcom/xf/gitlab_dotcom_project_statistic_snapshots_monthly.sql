with
    source as (

        select * from {{ ref("gitlab_dotcom_project_statistic_historical_monthly") }}
    )

select *
from source
