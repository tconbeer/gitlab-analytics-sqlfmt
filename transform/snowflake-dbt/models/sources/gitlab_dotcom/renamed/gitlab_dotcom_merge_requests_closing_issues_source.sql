with
    source as (

        select *
        from {{ ref("gitlab_dotcom_merge_requests_closing_issues_dedupe_source") }}

    ),
    renamed as (

        select distinct
            id::number as merge_request_issue_relation_id,
            merge_request_id::number as merge_request_id,
            issue_id::number as issue_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed
