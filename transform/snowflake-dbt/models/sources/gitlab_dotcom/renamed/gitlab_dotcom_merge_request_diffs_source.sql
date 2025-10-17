with
    source as (

        select *
        from {{ ref("gitlab_dotcom_merge_request_diffs_dedupe_source") }}
        where created_at is not null and updated_at is not null

    ),
    renamed as (

        select
            id::number as merge_request_diff_id,
            base_commit_sha,
            head_commit_sha,
            start_commit_sha,
            state as merge_request_diff_status,
            merge_request_id::number as merge_request_id,
            real_size as merge_request_real_size,
            commits_count::number as commits_count,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at
        from source

    )

select *
from renamed
