with
    source as (select * from {{ ref("gitlab_dotcom_approvals_dedupe_source") }}),
    renamed as (

        select
            id::number as approval_id,
            merge_request_id::number as merge_request_id,
            user_id::number as user_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed
