with
    source as (

        select * from {{ ref("gitlab_dotcom_merge_requests_closing_issues_source") }}

    )

select *
from source
