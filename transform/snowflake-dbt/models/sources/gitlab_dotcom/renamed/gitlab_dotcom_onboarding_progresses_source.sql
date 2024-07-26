with
    source as (

        select * from {{ ref("gitlab_dotcom_onboarding_progresses_dedupe_source") }}

    ),
    renamed as (

        select
            id::number as onboarding_progress_id,
            namespace_id::number as namespace_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            git_pull_at::timestamp as git_pull_at,
            git_write_at::timestamp as git_write_at,
            merge_request_created_at::timestamp as merge_request_created_at,
            pipeline_created_at::timestamp as pipeline_created_at,
            user_added_at::timestamp as user_added_at,
            trial_started_at::timestamp as trial_started_at,
            subscription_created_at::timestamp as subscription_created_at,
            required_mr_approvals_enabled_at::timestamp
            as required_mr_approvals_enabled_at,
            code_owners_enabled_at::timestamp as code_owners_enabled_at,
            scoped_label_created_at::timestamp as scoped_label_created_at,
            security_scan_enabled_at::timestamp as security_scan_enabled_at,
            issue_auto_closed_at::timestamp as issue_auto_closed_at,
            repository_imported_at::timestamp as repository_imported_at,
            repository_mirrored_at::timestamp as repository_mirrored_at
        from source

    )

select *
from renamed
