with
    source as (

        select * from {{ ref("gitlab_dotcom_deployment_merge_requests_source") }}

    )

select *
from source
