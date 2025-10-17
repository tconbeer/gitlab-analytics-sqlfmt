with
    source as (

        select * from {{ ref("gitlab_dotcom_merge_request_metrics_dedupe_source") }}

    ),
    renamed as (

        select

            id::number as merge_request_metric_id,
            merge_request_id::number as merge_request_id,

            latest_build_started_at::timestamp as latest_build_started_at,
            latest_build_finished_at::timestamp as latest_build_finished_at,
            first_deployed_to_production_at::timestamp
            as first_deployed_to_production_at,
            merged_at::timestamp as merged_at,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            latest_closed_at::timestamp as latest_closed_at,
            first_comment_at::timestamp as first_comment_at,
            first_commit_at::timestamp as first_commit_at,
            last_commit_at::timestamp as last_commit_at,
            first_approved_at::timestamp as first_approved_at,
            first_reassigned_at::timestamp as first_reassigned_at,

            pipeline_id::number as pipeline_id,
            merged_by_id::number as merged_by_id,
            latest_closed_by_id::number as latest_closed_by_id,
            diff_size::number as diff_size,
            modified_paths_size::number as modified_paths_size,
            commits_count::number as commits_count,
            added_lines::number as added_lines,
            removed_lines::number as removed_lines

        from source

    )

select *
from renamed
