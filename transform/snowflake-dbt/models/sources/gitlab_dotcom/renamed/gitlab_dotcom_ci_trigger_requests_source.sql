with
    source as (

        select * from {{ ref("gitlab_dotcom_ci_trigger_requests_dedupe_source") }}

    ),
    renamed as (

        select

            id::number as ci_trigger_request_id,
            trigger_id::number as trigger_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            commit_id::number as commit_id

        from source

    )

select *
from renamed
