with
    source as (

        select * from {{ ref("gitlab_dotcom_merge_request_blocks_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as merge_request_blocks_id,
            blocking_merge_request_id::number as blocking_merge_request_id,
            blocked_merge_request_id::number as blocked_merge_request_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
