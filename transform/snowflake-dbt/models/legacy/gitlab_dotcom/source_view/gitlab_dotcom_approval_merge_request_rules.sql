with
    source as (

        select * from {{ ref("gitlab_dotcom_approval_merge_request_rules_source") }}

    )

select *
from source
